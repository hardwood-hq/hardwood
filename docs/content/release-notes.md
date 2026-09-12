<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Release Notes

See [GitHub Releases](https://github.com/hardwood-hq/hardwood/releases) for downloads and more information.

## 1.1.0-SNAPSHOT

- `FilterPredicate` takes a `byte[]` literal on any binary column, through `eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq` and `in` ([#1181](https://github.com/hardwood-hq/hardwood/issues/1181)).

- A `String` literal filters a column that holds text — a `STRING`, an `ENUM`, a `JSON` or an unannotated `BYTE_ARRAY` — and throws `IllegalArgumentException` on every other binary column, where a `byte[]` or the annotation's own literal type filters instead ([#1181](https://github.com/hardwood-hq/hardwood/issues/1181)).

- A `String` literal that is not well-formed UTF-16 throws `IllegalArgumentException` when the predicate is built ([#1181](https://github.com/hardwood-hq/hardwood/issues/1181)).

- Every `FilterPredicate` factory rejects a null literal with a `NullPointerException` naming the argument ([#1181](https://github.com/hardwood-hq/hardwood/issues/1181)).

- `lt`, `ltEq`, `gt` and `gtEq` throw `IllegalArgumentException` on an `INTERVAL`, `GEOMETRY`, `GEOGRAPHY` or `NULL` column, whose values parquet-format puts in no order ([#1183](https://github.com/hardwood-hq/hardwood/issues/1183)).

- A comparison or set predicate on a leaf below a `VARIANT` group throws `IllegalArgumentException`; those leaves hold the encoded variant and take `isNull` and `isNotNull` only ([#1183](https://github.com/hardwood-hq/hardwood/issues/1183)).

- `not` over a predicate holding an `intersects` throws `IllegalArgumentException` at reader creation, where it used to throw `UnsupportedOperationException` ([#1183](https://github.com/hardwood-hq/hardwood/issues/1183)).

- A `BOOLEAN` column takes `lt`, `ltEq`, `gt` and `gtEq`, ordering `false` before `true`; they used to be answerable only through the record constructor and returned every non-null row ([#1183](https://github.com/hardwood-hq/hardwood/issues/1183)).

- `FilterPredicate.eq` and `notEq` take a `PqInterval` literal on an `INTERVAL` column ([#1183](https://github.com/hardwood-hq/hardwood/issues/1183)).

- `getString` and `ColumnReader.getStrings` throw `IllegalArgumentException` on a column that does not hold text, where they used to decode its stored bytes as characters ([#1196](https://github.com/hardwood-hq/hardwood/issues/1196)).

- An equality literal a column cannot hold throws `IllegalArgumentException`: an `Instant` or `LocalTime` finer than the column's time unit, a `BigDecimal` past its scale, a byte literal of a width a fixed-width column does not have, a `float` no IEEE half represents, and any literal outside the range of the `INT32` or `INT64` behind the column ([#1193](https://github.com/hardwood-hq/hardwood/issues/1193)).

- An ordered predicate whose literal the column cannot hold is answered exactly, where several such literals used to throw `ArithmeticException` ([#1193](https://github.com/hardwood-hq/hardwood/issues/1193)).

- `not` over a predicate that matches every non-null row, or over one that matches none, no longer returns the rows the comparison leaves out for being null ([#1193](https://github.com/hardwood-hq/hardwood/issues/1193)).

- A `TIME`, `INT` or `DECIMAL` annotation stored in a physical type that cannot hold it is dropped, and the column is read as its physical type: `TIME(MILLIS)` on `INT64`, `TIME(MICROS)` or `TIME(NANOS)` on `INT32`, `INT(8)`, `INT(16)` or `INT(32)` on `INT64`, `INT(64)` on `INT32`, and a `DECIMAL` with more digits than its `INT32`, `INT64` or `FIXED_LEN_BYTE_ARRAY` holds ([#1139](https://github.com/hardwood-hq/hardwood/issues/1139)).

- An `INT` annotation whose footer names a bit width other than 8, 16, 32 or 64, or names none at all, raises `ParquetReadException` instead of `IllegalArgumentException` ([#1139](https://github.com/hardwood-hq/hardwood/issues/1139)).

- A `ColumnReader` filter on a `FLOAT16` column no longer throws `ClassCastException`, and one on a struct leaf no longer reads a leaf that is null under a present struct as a value ([#1197](https://github.com/hardwood-hq/hardwood/issues/1197)).

- A `String`, `inStrings` or parquet-java `binaryColumn` literal on a `FIXED_LEN_BYTE_ARRAY` `DECIMAL` column that is narrower or wider than the column no longer drops row groups holding matching rows ([#1190](https://github.com/hardwood-hq/hardwood/issues/1190)).

- Row groups and pages are no longer pruned against `min` / `max` in a sort order the reader cannot read — a column annotated `INTERVAL`, `GEOMETRY`, `GEOGRAPHY`, `VARIANT`, `UNKNOWN`, `LIST` or `MAP`, or one whose file declares an unrecognized `ColumnOrder` ([#1179](https://github.com/hardwood-hq/hardwood/issues/1179)).

- `isNull` and `isNotNull` accept the name of a group — a struct, a `LIST` or a `MAP` — testing whether the group itself is present rather than one of its fields ([#977](https://github.com/hardwood-hq/hardwood/issues/977)).

- `FilterPredicate.in(String, double...)` filters `FLOAT` and `DOUBLE` columns by set membership ([#868](https://github.com/hardwood-hq/hardwood/issues/868)).

- Row-group and page statistics pruning no longer drops `NaN` rows that a floating-point predicate matches ([#1016](https://github.com/hardwood-hq/hardwood/issues/1016)).

- Statistics whose `min` sorts above its `max` no longer prune ([#1172](https://github.com/hardwood-hq/hardwood/issues/1172)).

- Statistics carrying a null count and no bounds are no longer reported as carrying deprecated `min` / `max` bounds ([#1172](https://github.com/hardwood-hq/hardwood/issues/1172)).

- A `ParquetFileReader` no longer retains a read's `RowGroupIterator` after the reader consuming it is closed ([#1170](https://github.com/hardwood-hq/hardwood/issues/1170)).

- A logical type annotation that a column's physical type cannot carry is now dropped, and the column is read as its physical type. Previously it surfaced as an `IllegalArgumentException` from whichever accessor first reached the column ([#1139](https://github.com/hardwood-hq/hardwood/issues/1139)). `FLOAT16` is defined as a two-byte payload, so a column annotated `FLOAT16` that declares three bytes is invalid; [parquet-format PR 606](https://github.com/apache/parquet-format/pull/606) specifies that readers ignore the annotation and use only the physical type. An annotation this version does not recognize is dropped the same way, so a file written against a newer format version can still be read. Both cases log a warning, naming the column and the reason, or the union field that was not recognized. **What changes for you:** `getFileSchema()` reports no logical type for such a column, `getValue` returns its physical value where it used to throw, and a logical accessor on it fails as it does on any unannotated column.

- A multi-file read opens each file as it reaches it, rather than opening every file when the reader is built, so the time to the first row no longer grows with the number of files ([#1107](https://github.com/hardwood-hq/hardwood/issues/1107)).
    - A later file's I/O errors, and any `SchemaIncompatibleException` its schema raises, now surface from the reading loop rather than from `ParquetFileReader.openAll(...)` or `build()` — always before any row of that file is returned. Code that catches those around reader construction alone should catch them around iteration too.

- Every `LogicalType` member has a static factory, and those are the documented way to construct one — `LogicalType.string()`, `LogicalType.decimal(18, 2)`, `LogicalType.timestamp(true, TimeUnit.MICROS)` ([#1074](https://github.com/hardwood-hq/hardwood/issues/1074)). The parameterless ones return a shared instance, which the reader now hands back instead of allocating a record per column while it decodes a footer. The record constructors still work.

- A `LocalDate` predicate requires the column to carry the `DATE` annotation, as its JavaDoc has always said ([#1141](https://github.com/hardwood-hq/hardwood/issues/1141)). The annotation went unchecked before, so a `LocalDate` against a plain `INT32` column compared epoch days against unrelated integers and returned rows answering a different question, with nothing raised. **What changes for you:** such a call now throws `IllegalArgumentException` at reader creation. A plain `INT32` column that does hold epoch days is filtered by the day itself — `gt("d", (int) date.toEpochDay())`.

- A `String` predicate on a `DECIMAL` or `FLOAT16` column compares as the column does — a `DECIMAL` by its unscaled value, a `FLOAT16` by the number its two bytes encode — rather than as a byte string ([#1142](https://github.com/hardwood-hq/hardwood/issues/1142)). Both order by the value their bytes stand for and record their statistics that way, so comparing byte-wise pruned row groups against bounds written in a different order and silently dropped matching rows. This is the comparison parquet-java applies, so a filter carried over through the compatibility shim answers the same. `inStrings` compares its probes the same way — which is also how parquet-java evaluates `In` — so a padded encoding of a `DECIMAL` probe is found; on a `FLOAT16` column each probe is compared as the half it encodes, and `in(double...)` accepts a `FLOAT16` column too.

- An ordered predicate on an `INT(bitWidth, isSigned = false)` column compares by unsigned magnitude, the order the column is written in ([#1144](https://github.com/hardwood-hq/hardwood/issues/1144)). The comparison was signed before, so `lt`, `gt` and their siblings returned wrong rows on a column holding values above 2^31, and bounds straddling that point read as inverted and were discarded — costing those columns row-group and page skipping as well.

- `print`, `convert`, `inspect` and `dive` spell a value of a given logical type the same way: decimals as plain strings (`0.0000001`, never `1E-7`), `INTERVAL` as `1mo 15d 3600000ms`, and `INT96` values and statistics as timestamps ([#1021](https://github.com/hardwood-hq/hardwood/issues/1021)).

- Control characters in values shown by `print`, `dive`, `inspect` and `info` render as `·`, so they no longer break table rows or reach the terminal ([#865](https://github.com/hardwood-hq/hardwood/issues/865)).

- A min/max statistic or dictionary entry that does not decode as its type renders in its stored form, `0x` hex or the stored integer, instead of failing `inspect` or blanking a `dive` screen ([#1021](https://github.com/hardwood-hq/hardwood/issues/1021)).

- `dive` renders unsigned integers inside structs, lists and maps as unsigned, and its physical toggle shows list elements' stored values ([#1021](https://github.com/hardwood-hq/hardwood/issues/1021)).

- `convert --format csv` quotes a field holding a carriage return ([#1021](https://github.com/hardwood-hq/hardwood/issues/1021)).

- `convert --format json` writes a non-finite Variant float as a JSON string and a Variant timestamp without a time zone without a trailing `Z` ([#1021](https://github.com/hardwood-hq/hardwood/issues/1021)).

**Breaking Changes:**

- `FilterPredicate.SignedBinaryColumnPredicate` is removed; a `DECIMAL` column is filtered with the `BigDecimal` factories ([#1190](https://github.com/hardwood-hq/hardwood/issues/1190)).

- The reader's exception model separates what the transport got wrong from what the file did, so a failure says whether trying again can help ([#1104](https://github.com/hardwood-hq/hardwood/issues/1104)).
    - `IOException` now means the transport — a read that failed, a connection reset — and is declared where the reader reaches the file: `RowReader.hasNext`/`next`/`close` and `ColumnReader.nextBatch`/`close`, as `ParquetFileWriter` has always declared it. **The canonical idiom is unaffected**, because `ParquetFileReader.open(...)` already declared `IOException` and so the enclosing method already handles it:

      ```java
      try (ParquetFileReader reader = ParquetFileReader.open(file);
           RowReader rows = reader.buildRowReader().build()) {
          while (rows.hasNext()) {
              rows.next();
          }
      }
      ```

      What does need a change is a method that receives an already-open reader and reads from it without otherwise touching `IOException` — a helper like `void print(RowReader rows)` — and any read inside a lambda whose functional interface forbids a checked exception, such as `forEach` or `Stream.map`.
    - A corrupt file now raises the new unchecked `ParquetReadException` rather than `IOException`: bad magic, a corrupt footer, a malformed page index, a misplaced dictionary page, a failed checksum, values that will not decode. `SchemaIncompatibleException` extends it. **This one is silent** — a `catch (IOException)` written for corruption keeps compiling and stops catching. It is released alongside the `throws` clauses deliberately, so the compile error brings you to the error handling the silent change would otherwise slip past.
    - A row group whose page-index region exceeds 2 GB now raises `UnsupportedOperationException` where it used to raise `IOException`. **Silent, like the one above** — a `catch (IOException)` written for it keeps compiling and stops catching.
    - `RowReader` and `ColumnReader` implement `Closeable`, as `ParquetFileWriter` and `InputFile` already did.
    - A page that will not decompress and a dictionary that will not decode now raise `ParquetReadException` too. Decompressing and parsing work on bytes already in memory, so they cannot fail at I/O and no longer say they can. **Silent, like the two above.**
    - A codec library that is absent, or a native one that will not load, now reaches you as the `UnsupportedOperationException` it always was on other paths. On the dictionary path it was being caught and reported as a failed read, which buried the message naming the dependency to add.
    - A column chunk stored in a separate file (the legacy split-file layout) and a file over 2 GB opened with the mmap-backed range cache now raise `UnsupportedOperationException` rather than `IOException`. Both files are correct; it is Hardwood that will not read them. **Silent.**
    - The writer raises the new unchecked `ParquetWriteException` when a compression codec rejects a page body, where it used to raise `IOException`. Nothing you passed was wrong and the destination is not involved, so retrying cannot help. **Silent.**
    - A corrupt value inside the metadata — a malformed bloom filter header, a geospatial bounding box missing a required field, a decimal scale or precision that cannot be, an unknown physical type, repetition type, codec or time unit — now raises `ParquetReadException` rather than `IllegalArgumentException` or `IllegalStateException`. These are the file being wrong, not your call being wrong. **Silent.** Both types keep their meaning for calls that really are mistakes, such as asking for a column outside the projection.

- `LogicalType.DecimalType` takes its precision before its scale, where it used to take scale first ([#1074](https://github.com/hardwood-hq/hardwood/issues/1074)). Every other decimal API takes them in that order — SQL's `DECIMAL(p, s)`, Arrow, Avro, Iceberg, parquet-cpp — and it is the order the annotation renders in. Only `parquet.thrift`'s field declaration and the APIs that mirror it read the other way. **This one is silent**, because a swapped call still compiles: `LogicalType.decimal(...)` rejects a scale above the precision, which catches a transposed pair at the call site, but a column whose scale equals its precision passes either way.

- `convert --format json` writes nested structs, lists, maps and repeated fields as native JSON objects and arrays instead of strings holding their display text ([#1021](https://github.com/hardwood-hq/hardwood/issues/1021)).

- `convert --format csv` writes a list or map cell as JSON text, as it does a Variant cell ([#1021](https://github.com/hardwood-hq/hardwood/issues/1021)).

- An unannotated byte array whose text starts with `0x` renders as `0x`-prefixed hex on every surface, so a `0x…` value always means bytes ([#1021](https://github.com/hardwood-hq/hardwood/issues/1021)).

## 1.1.0.Beta1 (2026-08-31)

[Announcement blog post](https://www.morling.dev/blog/parquet-file-write-support-bloom-filters-improved-performance-hardwood-1-1-0-beta1/) ·
[API changes](/api-changes/1.1.0.Beta1/)

Highlights of this release:

- Parquet files can be written — `RowWriter` a record at a time, `ColumnWriter` in aligned batches of primitive arrays — over the full type system, nested structs, lists and maps included
    - Every encoding and compression codec the format defines, bar the deprecated `LZ4` framing and `LZO`
    - Statistics and `nan_count` are written, page indexes and `SizeStatistics` are not
    - Footer key-value metadata and the `created_by` identifier are set on `ParquetFileWriter`, at any point until `close()` writes the footer
- Two push-down sources for skipping non-matching row groups
    - A column's Bloom filter, for `eq` and `in` predicates
    - The chunk's dictionary page, when its encoding stats show every data page is dictionary-encoded
- Further improved read performance
    - A fast path for clean [fixed-length `LIST` pages](https://www.morling.dev/blog/fast-path-for-fixed-length-lists-in-parquet/)
    - Bulk-unpacked `DELTA_BINARY_PACKED` miniblocks
    - Dictionary indices of 9 to 32 bits read a word at a time
    - All-present definition levels are no longer materialized
- Multi-file reading
    - Columns are matched by field path, so files may declare them in any order; a real mismatch raises `SchemaIncompatibleException` as the file opens
    - Physical `skip(N)` is a true global offset across the concatenated files
    - Per-file metadata through `ParquetFileReader.getFileCount()` and `getFileMetaData(int)`
- Correctness fixes
    - A `FilterPredicate` naming a group resolved to one of its leaves and returned wrong rows; a group name is now rejected with `IllegalArgumentException` at reader creation
    - Batch sizing accounts for list fan-out, so a high-fan-out repeated column no longer drains a whole file into a single batch and exhausts the heap
    - `GeographyType.algorithm` is read as the enum it is, so a non-spherical geography column no longer reads as `SPHERICAL`; an unrecognized algorithm reports `EdgeInterpolationAlgorithm.UNKNOWN`
    - Variant decoding bounds-checks lengths, guards 32-bit offset arithmetic, corrects the metadata offset-size read, and limits nesting depth
- The `hardwood` CLI moves from picocli/Quarkus to aesh, which makes it start faster
    - `dive` navigation is unified across all screens: every key moves the cursor, `▶` marks what `Enter` acts on
    - `convert` preserves types and NULL — JSON writes real scalars and `null`, CSV an empty field, with `--null-string` to override
    - `info` surfaces the file's key-value metadata

**Breaking Changes:**

- `Validity` moves to `dev.hardwood`, and `ColumnReader`'s raw `getDefinitionLevels()` / `getRepetitionLevels()` give way to the layer model ([#9](https://github.com/hardwood-hq/hardwood/issues/9), [#749](https://github.com/hardwood-hq/hardwood/issues/749))
- `ColumnIndex.nullPages()` and `nullCounts()` return `boolean[]` and `long[]`, and the canonical constructors of `ColumnChunk`, `ColumnIndex`, `ColumnMetaData` and `OffsetIndex` take further components — reading these records is unaffected, constructing them directly is not ([#607](https://github.com/hardwood-hq/hardwood/issues/607), [#856](https://github.com/hardwood-hq/hardwood/issues/856), [#903](https://github.com/hardwood-hq/hardwood/issues/903))
- `S3InputFile.length()` declares `throws IOException`, matching the `InputFile` contract ([#1072](https://github.com/hardwood-hq/hardwood/issues/1072))
- `hardwood help <command>` is removed — use `hardwood <command> --help` ([#686](https://github.com/hardwood-hq/hardwood/issues/686))

See the [1.1.0.Beta1 milestone](https://github.com/hardwood-hq/hardwood/milestone/8?closed=1) on GitHub for the full list of resolved issues.

Thank you to all contributors to this release: [Arnab Nandy](https://github.com/arnabnandy7), [Chandan Dhamande](https://github.com/nitrogen404), [Fawzi Essam](https://github.com/iifawzi), [Florian Meyer](https://github.com/alloutflo), [Gunnar Morling](https://github.com/gunnarmorling), [Hursh](https://github.com/hurshh), [Hyungun](https://github.com/chlgusrbs0), [Joshua Buss](https://github.com/chicagobuss), [Karen Barseghyan](https://github.com/k-barseghyan), [Kohinoor Gupta](https://github.com/kogupta), [Mehmet Turac](https://github.com/mturac), [Mingjie Zhao](https://github.com/ZhaoMJ), [Morax](https://github.com/fzlzjerry), [Nikulin Nikita](https://github.com/w3lld1), [Rion Williams](https://github.com/rionmonster), [Sebastian Legarraga](https://github.com/slegarraga), [Semyon Sinchenko](https://github.com/SemyonSinchenko), [Shaik Sameer](https://github.com/samsameer2804-cloud), [Shril Kumar](https://github.com/shril), [Ståle Pedersen](https://github.com/stalep).

## 1.0.0.Final (2026-06-25)

[Announcement blog post](https://www.morling.dev/blog/hardwood-1-0-fast-lightweight-apache-parquet-reader-for-the-jvm/) · [API changes](/api-changes/1.0.0.Final/)

Highlights of this release:

- Float and double row group and page pruning honors the file's column order, with `ColumnOrder` surfaced on the API; `ResolvedPredicate` float/double convenience constructors are now public
- Legacy list encodings from older writers — un-annotated repeated fields and 2-level lists — are recognized as lists
- MAP columns without a value field (key-only `key_value` groups) are read instead of throwing
- Sub-field projections into MAP values and VARIANT groups pull in the structural columns they require (the MAP's key, every VARIANT leaf)
- `AvroRowReader` honors column projections, and DECIMAL, UUID, UINT_32, and FIXED columns now read correctly
- The multi-file `Hardwood` entry point accepts a caller-supplied `HardwoodContext`, for control over decoder thread-pool sizing and sharing a context across readers
- `ColumnReader` and `RowReader` `close()` methods are now idempotent, fixing a close-time performance regression from Beta2
- Logical types render as Parquet-style annotation tokens (e.g. `STRING`)
- `hardwood dive` fails fast with a clear message when stdout is not an interactive terminal
- `hardwood print` short option names follow the conventional single-dash, single-character form (`-s`, `-w`, `-i`, `-d`); `--transpose` is long-only

See the [1.0.0.Final milestone](https://github.com/hardwood-hq/hardwood/milestone/7?closed=1) on GitHub for the full list of resolved issues.

Thank you to all contributors to this release: [Fawzi Essam](https://github.com/iifawzi), [Gunnar Morling](https://github.com/gunnarmorling), [Leo Chashnikov](https://github.com/RayanRal), [Mohamed Ibrahim Elsawy](https://github.com/mohamedibrahim54), [Rion Williams](https://github.com/rionmonster), [Yash Priyadarshan](https://github.com/yashpriyadarshan).

## 1.0.0.CR2 (2026-06-07)

[API changes](/api-changes/1.0.0.CR2/)

Highlights of this release:

- **Breaking:** `RowReaderBuilder.firstRow()` renamed to `skip()`, which now composes with a filter as a logical `OFFSET` — rows are skipped after the filter is applied
- Docker distribution of the `hardwood` CLI, published as a multi-arch image to GHCR
- Configurable read batch size for `ColumnReader` / `ColumnReaders`, with the default now sized adaptively from the projected column widths instead of a fixed record count
- TIMESTAMP accessors honor `isAdjustedToUTC`, with dedicated accessors for local (non-UTC) timestamps
- Stricter metadata validation: negative sizes, counts, and offsets are rejected, as are shredded Variant objects repeating a field across `typed_value` and `value`
- NaN-safe row group and page pruning for `float` and `double` columns
- Duplicate map keys resolve to the last value per the Parquet spec
- `head()` with a filter caps matched rows rather than scanned rows
- Legacy MAP columns from older parquet-mr / Hive / Impala writers (which annotate only the inner `key_value` group) are now recognized as maps
- API change reports are now published alongside the JavaDoc on the website

See the [1.0.0.CR2 milestone](https://github.com/hardwood-hq/hardwood/milestone/4?closed=1) on GitHub for the full list of resolved issues.

Thank you to all contributors to this release: [Alexei Zenin](https://github.com/AlexeiZenin), [Fawzi Essam](https://github.com/iifawzi), [Gunnar Morling](https://github.com/gunnarmorling), [Mohamed Ibrahim Elsawy](https://github.com/mohamedibrahim54).

## 1.0.0.CR1 (2026-05-31)

[Announcement blog post](https://www.morling.dev/blog/improved-column-reader-api-geospatial-support-hardwood-1-0-0-cr1-available/) · [API changes](/api-changes/1.0.0.CR1/)

Highlights of this release:

- **Breaking:** `ColumnReader` rebuilt around a layer model, with per-layer validity, offsets, and real-item-only sizing for nested data (see the [Layer Model](how-to/column-reader.md#reading-nested-data-the-layer-model) docs); `ColumnReader` is now marked `@Experimental`
- More performant evaluation of multi-column filter expressions
- Split-aware reading via `RowGroupPredicate.byteRange(...)`, for Hadoop-style split integrations
- Coordinated multi-column reads via `ColumnReaders.nextBatch()` / `getRecordCount()`
- Richer `RowReader` value model: by-index field access on `PqStruct`, key-based lookup and typed accessors on `PqMap`, typed `List` accessors on `PqList`, and additional variant accessors
- Float16 logical type support (readable values and filter predicates) and recognition of the `NullType` logical annotation
- First-cut geospatial support (GEOMETRY/GEOGRAPHY logical types and bounding-box metadata)
- Reading of local files larger than 2 GB
- CLI: exhaustive logical-type formatting; `hardwood dive`: faster navigation of large collections and corrected "go to latest" in the data preview

See the [1.0.0.CR1 milestone](https://github.com/hardwood-hq/hardwood/milestone/6?closed=1) on GitHub for the full list of resolved issues.

Thank you to all contributors to this release: [Carlos Sousa](https://github.com/CarlosEduR), [Fawzi Essam](https://github.com/iifawzi), [Gunnar Morling](https://github.com/gunnarmorling), [Manish](https://github.com/mghildiy), [Mohamed Ibrahim Elsawy](https://github.com/mohamedibrahim54), [muhannd Sayed](https://github.com/muhannd2004), [polo](https://github.com/polo7), [Prashant Khanal](https://github.com/prshnt), [Rion Williams](https://github.com/rionmonster), [Said Boudjelda](https://github.com/bmscomp).

## 1.0.0.Beta2 (2026-04-29)

[Announcement blog post](https://www.morling.dev/blog/variant-support-interactive-parquet-file-tui-hardwood-1.0.0.beta2-is-out/) · [API changes](/api-changes/1.0.0.Beta2/)

Highlights of this release:

- Interactive `hardwood dive` TUI for exploring Parquet files
- Parquet Variant logical type, including shredded reassembly
- Additional logical types: INTERVAL, MAP/LIST, INT96 timestamps
- Faster reads via a parallel per-column pipeline and per-column in-page row skipping
- Reduced S3 traffic via byte-range caching, coalesced GETs, and small-column fetches
- Unified reader API based on builders
- CLI with reorganized `inspect` subcommands

See the [1.0.0.Beta2 milestone](https://github.com/hardwood-hq/hardwood/milestone/3?closed=1) on GitHub for the full list of resolved issues.

Thank you to all contributors to this release: [André Rouél](https://github.com/arouel), [Brandon Brown](https://github.com/brbrown25), [Bruno Borges](https://github.com/brunoborges), [Fawzi Essam](https://github.com/iifawzi), [Gunnar Morling](https://github.com/gunnarmorling), [Manish](https://github.com/mghildiy), [polo](https://github.com/polo7), [Rion Williams](https://github.com/rionmonster), [Sabarish Rajamohan](https://github.com/sabarish98), [Trevin Chow](https://github.com/tmchow).

## 1.0.0.Beta1 (2026-04-02)

[Announcement blog post](https://www.morling.dev/blog/hardwood-reaches-beta-s3-predicate-push-down-cli/) · [API changes](/api-changes/1.0.0.Beta1/)

Highlights of this release:

- S3 and remote object store support with coalesced reads
- CLI tool for inspecting and querying Parquet files
- Avro `GenericRecord` support via the `hardwood-avro` module
- Row group filtering with predicate push-down and page-level column index filtering
- `InputFile` abstraction for pluggable file sources
- S3 support and filtering in the parquet-java compatibility layer
- Project documentation site

See the [1.0.0.Beta1 milestone](https://github.com/hardwood-hq/hardwood/milestone/1?closed=1) on GitHub for the full list of resolved issues.

Thank you to all contributors to this release: [Arnav Balyan](https://github.com/ArnavBalyan), [Brandon Brown](https://github.com/brbrown25), [Gunnar Morling](https://github.com/gunnarmorling), [Manish](https://github.com/mghildiy), [Nicolas Grondin](https://github.com/ngrondin), [Rion Williams](https://github.com/rionmonster), [Romain Manni-Bucau](https://github.com/rmannibucau), [Said Boudjelda](https://github.com/bmscomp).

## 1.0.0.Alpha1 (2026-02-26)

[Announcement blog post](https://www.morling.dev/blog/hardwood-new-parser-for-apache-parquet/)

Highlights of this release:

- Zero-dependency Parquet file reader for Java
- Row-oriented and columnar read APIs
- Support for flat and nested schemas (lists, maps, structs)
- All standard encodings (RLE, DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT, etc.)
- Compression: Snappy, ZSTD, LZ4, GZIP, Brotli
- Projection push-down, parallel page pre-fetching, and memory-mapped file I/O
- Multi-file reader and `parquet-java` compatibility layer
- Optional Vector API acceleration on Java 22+
- JFR events for observability
- BOM for dependency management

Thank you to all contributors to this release: [Andres Almiray](https://github.com/aalmiray), [Gunnar Morling](https://github.com/gunnarmorling), [Rion Williams](https://github.com/rionmonster).
