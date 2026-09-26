# Writer validation

How the writer's output is shown to be readable by implementations other than Hardwood: the strict-reader interop gate, the DuckDB differential, the writer's own round-trip tests, and the coverage assertion that holds the gate to everything the writer can produce. The write path itself is in [WRITER.md](WRITER.md), the input contract in [WRITER_INPUT.md](WRITER_INPUT.md), and the encodings, codecs and statistics under test in [WRITER_ENCODING.md](WRITER_ENCODING.md). The read-direction differential harness and the integration-test wiring are in [TESTING.md](../TESTING.md#differential-testing).

## Reader tiers

Every produced shape is read back by readers of graded strictness. A strict reader establishes conformance; a lenient one adds a second decoder's agreement on the values; Hardwood's own reader pins details the others do not surface.

| Tier | Reader | Module | What it establishes |
|---|---|---|---|
| Strict | parquet-java, through its Group API and footer reader | `parquet-testing-runner` | The bytes conform: what the canonical implementation accepts is the operative definition of a conformant file |
| Lenient | DuckDB, through `read_parquet` and `parquet_metadata` | `core` | A second, independent decoder agrees on values and statistics |
| Round trip | Hardwood's `ParquetFileReader` and `RowReader` | `core` | Reader and writer agree, including on details neither external reader exposes (exact encodings, statistics, range extremes) |

A permissive consumer agreeing on the values does not establish conformance. An encoder can emit a stream that only lenient decoders accept, and neither a Hardwood round trip nor DuckDB can see it. The reference case is the **single-entry dictionary defect**: a column chunk whose dictionary holds exactly one entry, written as an `RLE_DICTIONARY` index stream with no run header, so the page body is the bit-width byte `0x00` and nothing else. Hardwood's decoder short-circuits on a zero bit width and never reads the stream, and DuckDB is lenient in the same place, so both accept a file that parquet-java and PyArrow reject. The strict tier exists for this defect class.

PyArrow also catches this class, with failure modes that differ from parquet-java's per physical type. It is not part of any automated suite; reading Hardwood-written files with PyArrow is an ad-hoc verification step.

## Interop gate

The gate writes a file with Hardwood and reads it back with parquet-java. It needs no dependency beyond what `parquet-testing-runner` carries for the read direction (`parquet-avro`, `parquet-hadoop`, `hadoop-common` at test scope) and does not touch the `parquet-testing` fixture repository: its inputs are files Hardwood writes.

### Reading through the Group API, not Avro

The module's read-direction comparison reads reference rows through `AvroParquetReader`, which maps Parquet onto Avro's type system. That mapping cannot represent several annotations the writer emits — `UUID`, `FLOAT16`, `INTERVAL`, the unsigned integer widths — so an Avro-based gate would have to exclude parts of the matrix for reasons unrelated to whether the bytes are conformant.

The gate reads through parquet-java's Group API instead — `ParquetReader<Group>` over `GroupReadSupport` — which materializes any valid file with no object model in the way. A read failure therefore means the bytes are bad. The Group path exercises the same decoders the Avro path does, including the `DictionaryValuesReader` that rejects the single-entry dictionary defect.

The footer is read separately through parquet-java's `ParquetFileReader`, in both its decoded form and its raw Thrift form, so the gate also covers metadata a value comparison cannot see.

`ParquetJavaReader` wraps the Group reader, both footer reads, the page walk and the `created_by` check, so the parquet-java surface the gate depends on sits in one place. It is separate from `Utils`, which serves the read direction and carries the Avro comparison.

### What the gate asserts

Per file:

1. **It reads.** parquet-java materializes every row without throwing. This alone is the single-entry dictionary check.
2. **The values agree.** Every value parquet-java produces equals the value that was written, not merely what Hardwood reads back. Nulls, list and map cardinalities, and empty-versus-absent repeated values are compared as written.
3. **The metadata agrees.** The column-chunk statistics parquet-java exposes — `min`, `max`, `null_count` — match the written data under parquet-java's own comparator, which it derives from the column's annotation. An order the two implementations disagree about produces bounds that would prune a row group holding a matching row. Bounds are asserted as the true extremes of the written values, reduced with that comparator, rather than merely as `min <= max`: most annotations carry values that sort identically under every candidate order, so a consistency check passes whichever order the writer used. An annotation with an undefined order (`INTERVAL`) is asserted to carry no bounds.
4. **The raw footer agrees.** Fields parquet-java folds into defaults or does not surface are read from the Thrift structure:
   - `column_orders`, one `TYPE_DEFINED_ORDER` per leaf. parquet-java defaults a missing column order to type-defined for every type but `INT96` and `INTERVAL`, so its decoded schema reports `TYPE_DEFINED_ORDER` whether the field reached the wire or not, and every other assertion would hold on a file that dropped the list.
   - `distinct_count`, present exactly for the chunks that know their cardinality, since parquet-java's `Statistics` does not expose it.
   - `encoding_stats`, which must agree with the pages the walk found: the same data-page encodings, a dictionary entry exactly where the chunk has a dictionary page, and page counts that add up.
   - `key_value_metadata`, including a key with no value and a file given no entries, which must carry no field at all. The consumers of this field (`ARROW:schema`, pandas and table-format stamps) are never Hardwood, so a malformed `list<KeyValue>` Hardwood's own reader accepted would pass both halves of a round trip.
5. **The writer is identifiable.** `VersionParser` parses `created_by` into the `hardwood` application with a semantic version; `CorruptStatistics.shouldIgnoreStatistics` returns false for `BINARY` and `FIXED_LEN_BYTE_ARRAY`; and `CorruptDeltaByteArrays.requiresSequentialReads` returns false for `DELTA_BYTE_ARRAY`. A reader that cannot parse `created_by` applies its writer-specific workarounds by default: under the PARQUET-251 heuristic parquet-java discards the deprecated `min`/`max` of a `BINARY` or `FIXED_LEN_BYTE_ARRAY` column from a writer it cannot identify. Hardwood writes only `min_value`/`max_value`, which that heuristic does not gate, so a parseable identifier keeps the outcome independent of which statistics fields the writer emits. Readers that go through Hardwood or DuckDB see a well-formed file either way.
6. **The case covered what it claims to.** The encodings show that the case produced the dictionary or `PLAIN` chunk it exists to cover, and the layout cases walk parquet-java's page readers to count the data pages they crossed. Without this, a change to the writer's encoding choice or page sizing would silently empty an axis rather than fail it.

   This reads the **page** value encodings, not the column chunk's `encodings` list, which always contains `PLAIN` because a dictionary page body is itself `PLAIN`. Within a chunk the page encoding never varies, which is the guarantee of the row-group-wide dictionary decision ([WRITER_ENCODING.md](WRITER_ENCODING.md)). Across chunks the case's intent is pinned on the first, full chunk: a case whose values argue against a dictionary produces none anywhere, while one whose values argue for it may write a short trailing chunk `PLAIN`. The page walk also rejects any page that is not DataPage V1, the only page format the writer produces.

### The flat matrix

The floor is a **single-entry dictionary per physical type** in every repetition shape. `BOOLEAN`, never dictionary-encoded, writes it `PLAIN`.

Beyond the floor, the matrix varies one axis at a time against a representative base rather than taking the full cross product, which would multiply to hundreds of files for coverage the axes already give independently. An axis is swept across all seven writable physical types (`BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`), since the value encoders are per type and that is where an encoding defect lives — except where the axis is defined over only some of them, and there the sweep is restricted to those.

| Axis | Values |
|------|--------|
| Repetition | `REQUIRED`; `OPTIONAL` all-present; `OPTIONAL` with interleaved nulls; `OPTIONAL` all-null |
| Encoding | multi-entry dictionary; an all-distinct column the size comparison writes `PLAIN`; `PLAIN` named as the policy |
| Optional encoding | `DELTA_BINARY_PACKED`; `DELTA_LENGTH_BYTE_ARRAY`; `DELTA_BYTE_ARRAY`; `BYTE_STREAM_SPLIT` — one case per legal (encoding, physical type) pair, taken from `EncodingSupport`, in every repetition shape |
| Optional encoding × codec | each of those four encodings, on one type it is legal for, against every codec parquet-java reads, in every repetition shape |
| `FIXED_LEN_BYTE_ARRAY` length | 2, 8, 12 and 16 bytes against every encoding legal for the type |
| Codec | every codec the writer produces except `BROTLI` |
| Layout | one page (pinned exactly); several pages; several row groups |
| Write path | the columnar batch entry point; the row-oriented `RowWriter` |

The three axes whose defect lives in the value encoder — the single-entry floor, the optional encodings, and those encodings crossed with the codecs — are each swept across all four repetition shapes, because what an encoder is handed differs by shape: a `REQUIRED` column has no definition-level stream, and an all-null one gives the encoder an empty range and the codec a page body with no values in it. The codec axis checks framing: each codec has a raw block form and a wrapped one, and the wrapped form round-trips against Hardwood's own reader while being the wrong bytes for everyone else.

One enumerated case sits beside the sweep: a page whose values all resolve to dictionary index 0 inside a multi-entry dictionary, which declares a zero index bit width that no swept case produces.

`BROTLI` is the one codec the writer produces that the gate omits. parquet-java resolves a codec by a Hadoop class name, and for `BROTLI` that class ships in neither parquet-java nor Hadoop, but in an unmaintained third-party artifact whose native binaries cover a few platforms only; putting it on the classpath would make the gate's result depend on the architecture it runs on. `BROTLI` output is covered against DuckDB by `WriterDifferentialTest`, and `WriterInteropTest.parquetJavaHasNoBrotliCodec` pins the reason, so a parquet-java that gains the codec fails there and `BROTLI` rejoins the axis.

### Enumerated groups

Two groups are enumerated rather than swept, because their shapes differ too much to parameterize:

- **Nesting** — `REQUIRED` and `OPTIONAL` structs, nested structs, lists (including empty lists, absent lists, null elements, lists of lists, lists of structs), maps (including maps of lists and maps of structs), structs enclosing lists and maps, lists crossing page and row-group boundaries, both legacy two-level list spellings reaching the writer from a schema read off an existing file, and the row-written forms of these shapes including a nested run across staged batches. These are the shapes that carry repetition and definition level streams, which a flat column does not exercise. The legacy spellings need an external reader in particular: Hardwood's reader accepts them by design and would agree with a file only it can read.
- **Logical types** — every annotation the writer emits, including the `UNKNOWN` of a `NullType` column, read back through parquet-java's schema so both the annotation and the values it governs are checked, plus the three sort orders a table of well-behaved values cannot discriminate on its own: unsigned integers, binary `DECIMAL`, and a `STRING` across the signed-byte boundary.

The row-oriented layer's logical-type conversion (`PhysicalValueConverter`) has its own oracle: the same annotation table driven through `RowWriter` setters, with expectations taken from Avro's conversions (on the classpath through `parquet-avro`) or spelled out from the value's definition, rather than from `LogicalTypeConverter`, whose inverse it is. An error the two share — a byte order, a unit scale, a sign extension — survives every Hardwood round trip.

`WriterAnnotationCoverageTest` covers the annotations at the points the hand-written table leaves out, generated from the coverage domain (below): each annotation in both storage forms, with the page encoding held to the form the case is for, and each annotation at the ends of its declared range and at a point inside it, read back value by value through parquet-java. The ends of a range are where the statistics comparator is fragile: an unsigned `INT(32)` maximum is stored as `-1` and must compare unsigned, a binary `DECIMAL` bound needs sign extension, and a `BYTE_ARRAY` maximum of all-`0xff` bytes past the truncation length must truncate and increment.

Tests: `WriterInteropTest`, `WriterNestedInteropTest`, `WriterLogicalTypeInteropTest`, `RowWriterLogicalTypeInteropTest`, `WriterFooterMetadataInteropTest`, `WriterAnnotationCoverageTest` (all `parquet-testing-runner`).

## DuckDB differential

`WriterDifferentialTest` is the inverse of the read-direction differential: Hardwood writes a file and DuckDB reads it through `read_parquet('<path>')`. Each row carries a synthetic index column `r`, and the comparison orders by it. The cases cover every physical type, boundary values (signed extremes, `NaN` and signed zeros, all-null columns), nested structs, lists and maps, dictionary and `PLAIN` chunks, the optional encodings, every codec the writer produces including `BROTLI`, multiple row groups, and the statistics DuckDB's `parquet_metadata()` decodes. One case filters on the written statistics over banded row groups, so a wrong bound shows as a dropped matching row.

Tests: `WriterDifferentialTest`.

## Round trips

The `core` round-trip tests write with Hardwood and read back with Hardwood. They are the fast inner-loop check and pin reader-writer agreement on what the external readers do not surface. Beyond value and null equality, two assertions are specific to this tier:

- **Symmetry of ranges.** Every value the writer accepts reads back as the identical value. `WriterReaderSymmetryTest` writes the extremes each annotation admits, where the writer's accepted range and the reader's materializable range would part company.
- **Refusal outside the range.** A value outside its annotation's declared range reaches no file, so no external reader can observe it. `WriterAnnotationRangeTest` holds both write APIs to refusing it, with bounds pinned as its own constants rather than derived from `LogicalTypeValueRange`, the class the writer applies.

`RowWriterEquivalenceTest` asserts that the same logical data written through `ColumnWriter.writeBatch` and through `RowWriter` produces byte-identical files, which is what lets the gate treat the row layer as an entry point rather than a second write path.

Tests: `WriterRoundTripTest`, `WriterFixedWidthTypeRoundTripTest`, `WriterVariableWidthTypeRoundTripTest`, `WriterNestedRoundTripTest`, `WriterLogicalTypeRoundTripTest`, `RowWriterRoundTripTest`, `WriterReaderSymmetryTest`, `WriterAnnotationRangeTest`, `RowWriterEquivalenceTest`.

## Coverage assertion

Every combination the writer can produce is either produced by some gate test or waived with a stated reason. A combination that is neither fails the build. This makes the gate's growth mechanical: an increment that widens the writer without widening the tests fails in the commit that widens it. Three parts make it up: an observation recorded from the bytes each test produced, a domain derived from the writer's own capabilities, and a verdict that diffs them.

### Observation

Every gate test funnels its file through `ParquetJavaReader`, and each of its entry points opens with `observe`, which walks the file once — however many entry points one test uses — and records into `CoverageRegistry`:

- per column chunk, the physical type, the `FIXED_LEN_BYTE_ARRAY` length where it has one, the annotation, the encodings its data pages declared, the codec its bodies are compressed with, and its repetition shape;
- per group node, the annotation it carries, which is where `LIST` and `MAP` live.

No test opts in; every test added later contributes by running. What is recorded is what parquet-java found in the file, never what the test intended to write: a writer that silently stopped producing an encoding would still satisfy a registry keyed on intent. A chunk's storage form is `DICTIONARY` when its data pages declared `RLE_DICTIONARY`.

The repetition shape is the one thing the file does not state outright. The three `OPTIONAL` shapes share a descriptor and differ only in the definition levels the values produced, so they are told apart by the chunk's null count against its value count, the latter counting nulls as the format's `num_values` does. A chunk whose statistics state no null count leaves its shape unrecorded rather than guessed. A column under a repeated field is recorded as `REPEATED`.

### Domain

Hand-listing the domain would reproduce the problem it exists to solve, so `CoverageDomain` derives each dimension from the writer:

| Dimension | Derived from |
|---|---|
| Physical types | `PhysicalType.values()`, probed by asking the writer to write each; `INT96` is refused |
| Page encodings | `ColumnEncoding` × its per-type legality in `EncodingSupport`, resolved to the encodings a page can declare: `AUTO` reaches `PLAIN` and, where a dictionary is possible, `RLE_DICTIONARY`; every other policy names one outright |
| Codecs | `CompressionCodec.values()`, probed; `LZO` and the Hadoop-framed `LZ4` are refused |
| Repetition shapes | the four flat shapes of `InteropCase.Nullability` |
| Annotation kinds | the sealed `LogicalType` hierarchy |
| Annotation parameters | declared per parameterized kind (below) |

The resolution from policy to page encoding is a switch expression, so a policy added to `ColumnEncoding` is a compile error there rather than a silently missing encoding.

A probe catches only the two exceptions the writer documents as refusals — `UnsupportedOperationException` for a capability it does not have, and `IllegalArgumentException` for a configuration a schema cannot carry. Anything else propagates, because a probe that swallowed an unexpected failure would drop the capability from the required set and leave the verdict passing having asserted less.

The domain reads the writer's own capability tables: `EncodingSupport` and `LogicalTypeValueRange` live in `dev.hardwood.internal.writer`, which the test module depends on directly. A mirrored copy would drift, which is the defect this assertion exists to catch.

The sealed hierarchy is what makes the domain grow on its own, but `CoverageDomain.annotations()` enumerates the parameterized points by hand. `WriteCoverageVerdictTest` therefore asserts that every member of `LogicalType` is either required by the domain or refused by the writer. The one refused member, `VariantType`, is excluded by asking the writer rather than by listing it: the test probes that the writer still refuses a column carrying it and holds the refusal to the reason it gives. "The writer threw" would not be enough — a group annotation on a primitive column is refused whatever the writer supports of it, so `LIST` and `MAP` are refused there too although both are fully written. Matching the reason makes the probe fail on the day the writer builds `VARIANT` groups.

### Required projections

The full cross product — physical types × encodings × codecs × repetition shapes × annotations — is neither reachable nor meaningful. The domain instead requires a set of pairwise projections, each admitted because a defect class lives in that pair and in no smaller one:

| Projection | The defect it targets |
|---|---|
| physical type × page encoding | The value encoders are per type. This is the single-entry dictionary class: a stream only a lenient decoder accepts. |
| page encoding × codec | Framing over an unusual page body. |
| physical type × repetition shape | The level streams and the value stream are written together and read together. |
| `FIXED_LEN_BYTE_ARRAY` length × page encoding | `BYTE_STREAM_SPLIT` scatters by byte position and `DELTA_BYTE_ARRAY` shares prefixes; both are length-sensitive. |
| annotation × carrier × storage form | An annotation's comparator governs the chunk's bounds in either storage form, and the dictionary path reaches those bounds through a different accumulator. Group annotations (`LIST`, `MAP`) take a single group cell. |

This holds the writer's capabilities to a few hundred cells against a cross product in the tens of thousands. The projections accumulate independently, so the flat matrix fills the first four without being restructured, and `WriterAnnotationCoverageTest` fills the fifth from the same `CoverageDomain.annotations()` the verdict requires — an annotation added to the writer produces a case and a requirement in the same commit.

The carrier is part of the annotation cell because it is part of what the annotation means: a `DECIMAL(1, 0)` over an `INT32` is bounded by arithmetic on the precision and the same annotation over a `BYTE_ARRAY` by the magnitude its bytes spell. An `UNKNOWN` column holds no value to intern, so it requires only the non-dictionary form.

The parameterized kinds declare their own points, chosen so that each parameter varies independently of the others:

| Kind | Required points |
|---|---|
| `IntType` | {8, 16, 32, 64} × {signed, unsigned} |
| `TimeType` | {MILLIS, MICROS, NANOS} × {adjusted, local} |
| `TimestampType` | {MILLIS, MICROS, NANOS} × {adjusted, local}, on `INT64` and on `FIXED_LEN_BYTE_ARRAY(12)` |
| `DecimalType` | per carrier — `INT32`, `INT64`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` — at precision 1 and the carrier's maximum, each at scale 0 and at scale equal to the precision |

The carrier maximum is found by asking the writer; `BYTE_ARRAY` accepts any precision and is capped at the maximum of a sixteen-byte fixed carrier. Scale equal to precision is the point at which a bound derived by arithmetic on the precision overflows.

There is no projection over the value boundaries an annotation declares. Those values are written and read back value by value by `WriterAnnotationCoverageTest`, which is a stronger statement than a cell; a cell recording that the case ran would be derived from intent rather than from bytes. Values outside the range reach no file, so they are not a shape this assertion can observe; `WriterAnnotationRangeTest` owns them. Nested shapes are recorded as `REPEATED` but not required: they vary the level streams rather than the value encoders, and `WriterNestedInteropTest` enumerates them.

### Waivers

A cell that cannot be covered is waived in `CoverageWaivers` as a (cell, reason) pair, the reason naming where the capability is covered instead:

| Waived cells | Reason |
|---|---|
| every page encoding × `BROTLI` | parquet-java cannot resolve the codec class; covered against DuckDB by `WriterDifferentialTest`, reason pinned by `WriterInteropTest.parquetJavaHasNoBrotliCodec` |
| `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)`, both storage forms | the pinned parquet-java rejects the pairing while parsing the footer; covered by `WriterFlba12TimestampTest` (`core`) and `Flba12TimestampTest`, reason pinned by `Flba12TimestampTest.parquetJavaRejectsTheAnnotation` |

A waiver is a claim about the world, so it is checked in both directions: **a waived cell that is nonetheless observed fails the verdict**. Without that, a waiver outlives its reason — a parquet-java release that gains the codec would leave `BROTLI` waived by inertia.

### The verdict

`WriteCoverageVerdictTest` merges the observations and asserts: the run produced observations at all (an empty set means the recording execution did not run); every required cell is observed or waived; no waived cell is observed; every `LogicalType` member is required or refused; and every observed cell spells a projection the domain knows, so a spelling mismatch between registry and domain shows as itself rather than as a gap. A failure names the empty cells grouped under their projection. `target/write-coverage-report.txt` states how much of each projection was reached.

Tests: `WriteCoverageVerdictTest` (parquet-testing-runner).

## Where it runs

The gate and the coverage assertion live in `parquet-testing-runner`, beside the read-direction comparison they invert, and run in the PR build with that module. The DuckDB differential and the round trips run in `core`'s test suite.

The verdict spans test classes, so it cannot be an `@AfterAll`, and Surefire may fork per class, so it cannot rely on a static registry surviving to the end of the run:

- `WriteCoverageListener`, a `TestExecutionListener` registered through `META-INF/services`, writes what the run recorded to `target/write-coverage/` as the test plan ends, under a name unique to the process so that forks do not overwrite one another.
- The module's POM binds `maven-clean-plugin` to `process-test-classes` against that one directory. The build empties it rather than the listener, because every fork loads the same listener and one that cleared on start would delete its siblings' files.
- The default Surefire execution excludes `WriteCoverageVerdictTest`. A second execution runs it alone with `hardwood.writeCoverage=verify`, under which the listener stands down rather than adding its own cells to what it is about to judge. The verdict also joins its own JVM's registry, so running the whole module inside an IDE reaches the same conclusion.
- The second execution sits in the `write-coverage-verdict` profile, active only when `test` is unset. `-Dtest=...` overrides every execution's includes and excludes, so a verdict execution left in such a build would re-run the selection and judge a subset it was never given.

## Boundaries

- The strict tier is parquet-java alone. PyArrow is not automated, and no other implementation (arrow-rs, parquet-go) reads Hardwood output in CI.
- No property-based or randomized test drives random schemas and data through the gate or the differential; coverage comes from the sweep, the enumerated groups and the annotation domain.
- Nested shapes are enforced by enumeration in `WriterNestedInteropTest`, not by the coverage assertion.
- The coverage assertion runs only when `parquet-testing-runner` runs whole; a `-Dtest` selection skips it.
- `BROTLI` and `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` have no strict-reader coverage while the pinned parquet-java cannot read them.
- Footer features the writer does not produce (page index, Bloom filters) have no gate assertions; each gains them in the change that adds it (#1291).
