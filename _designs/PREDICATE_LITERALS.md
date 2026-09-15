# Predicate literals

**Status: Implemented**

Which predicates a column takes, with which literals, and which rows they match. The public
surface is `FilterPredicate`'s factories; `reference/query-controls.md` states the same rule for
users.

## Rule

The examples below use four columns:

```
d    DATE
ts   TIMESTAMP(MICROS, isAdjustedToUTC = true)
dec  DECIMAL(9, 2) over BYTE_ARRAY; row A stores 1.25 as 7D, row B as 00 7D
u    INT(32, isSigned = false)
```

1. **A literal is a value an accessor returns for the column.** A column takes the value of its
   logical accessor (`LocalDate`, `Instant`, `LocalDateTime`, `LocalTime`, `BigDecimal`, `UUID`,
   `PqInterval`, a `float` on `FLOAT16`, a `String` where `getString` reads the column) and the
   value of its physical accessor (`boolean`, `int`, `long`, `float`, `double`, `byte[]`). Literal
   types do not widen.
   ```java
   eq("d", LocalDate.of(2026, 1, 1))   // ✓ getDate
   eq("d", 20454)                      // ✓ getInt, the epoch day
   eq("d", 20454L)                     // ✗ no accessor returns a long
   eq("dec", "1.25")                   // ✗ getString does not read a DECIMAL
   ```
2. **A literal matches a row when it denotes the value the row holds.** A typed literal denotes a
   value of the column's type, an `int` or `long` the stored integer (the bit pattern on an
   unsigned column), and a `byte[]` the stored bytes, whatever the annotation reads them as.
   ```java
   eq("dec", new BigDecimal("1.25"))   // rows A and B: both hold 1.25
   eq("dec", new byte[] { 0x7D })      // row A only: B stores other bytes
   ```
3. **A literal takes the ordered operators where it compares in the column's order.** Typed
   literals compare by value, `int` and `long` signed (unsigned on an unsigned `INT` column), and
   `byte[]` unsigned lexicographically. That is the order of every binary type except `DECIMAL`,
   `FLOAT16`, `INT96` and `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)`, which order by the value
   their bytes encode; there a `byte[]` takes equality and the set form only. A type that defines no
   order takes no ordered operator.
   ```java
   lt("dec", new BigDecimal("2.00"))   // ✓ by value
   lt("dec", new byte[] { 0x7D })      // ✗ a DECIMAL does not order as its bytes
   lt("u", -1)                         // ✓ unsigned: every row but one storing 0xFFFFFFFF
   ```
4. **An equality literal must be a value the column can hold; an order literal may be any value.**
   ```java
   lt("ts", Instant.parse("2026-01-01T00:00:00.000000500Z"))   // ✓ rows up to .000000
   eq("ts", Instant.parse("2026-01-01T00:00:00.000000500Z"))   // ✗ finer than MICROS
   ```

Every literal type with `eq` also has a set form, `in(column, values...)`, whose values are
equality literals, except `boolean`: `eq`, `notEq` and `isNotNull` already express every set of
two values.

## What can be filtered

- **Leaf columns**, at the top level or inside structs. A leaf below a `LIST` or a `MAP` occurs
  many times per row and is rejected.
- **Groups** (struct, `LIST`, `MAP`, `VARIANT`) through `isNull` / `isNotNull`, testing whether the
  group is present. The leaves below a `VARIANT` group hold the encoded variant, which no accessor
  reads, and take `isNull` / `isNotNull` only.
- **Annotations** as the reader sees them. A legacy `converted_type` alone is promoted to its logical
  type. An annotation the physical type cannot carry, or one the reader does not recognize, is
  dropped, and the column follows its physical type's row.

## Per column type

"All" means `eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq`, `in` and `not(in)`. Every column also takes
`isNull` / `isNotNull`.

| Column | Literals | Operators | Compared as |
|---|---|---|---|
| `BOOLEAN` | `boolean` | all but `in` | `false` < `true` |
| `INT32`, `INT64`, signed `INT(n)` | `int` / `long` | all | signed |
| unsigned `INT(n)` | `int` / `long`, the stored bit pattern | all | unsigned |
| `FLOAT`, `DOUBLE` | `float` / `double` | all | `Float.compare` / `Double.compare` |
| `FLOAT16` | `float`; `byte[]` of 2 | all; `byte[]`: `eq`, `notEq`, `in` | the half, as `Float.compare` |
| `DATE` | `LocalDate`, `int` | all | epoch day |
| `TIME` | `LocalTime`, `int` (millis) / `long` (micros, nanos) | all | count of the unit |
| `TIMESTAMP` over `INT64`, UTC | `Instant`, `long` | all | count of the unit |
| `TIMESTAMP` over `INT64`, local | `LocalDateTime`, `long` | all | wall-clock count of the unit |
| `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` | `Instant` (UTC) or `LocalDateTime` (local); `byte[]` of 12 | all; `byte[]`: `eq`, `notEq`, `in` | signed 96-bit count of the unit |
| `INT96` | `Instant`; `byte[]` of 12 | all; `byte[]`: `eq`, `notEq`, `in` | the instant |
| `DECIMAL` over `INT32` / `INT64` | `BigDecimal`; `int` / `long` unscaled | all | the number |
| `DECIMAL` over `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY(n)` | `BigDecimal`; `byte[]` (of `n`) | all; `byte[]`: `eq`, `notEq`, `in` | the number |
| `STRING`, `ENUM`, `JSON`, unannotated `BYTE_ARRAY` | `String`, `byte[]` | all | unsigned bytes |
| `BSON`, unannotated `FIXED_LEN_BYTE_ARRAY` | `byte[]` | all | unsigned bytes |
| `UUID` | `UUID`, `byte[]` of 16 | all | unsigned bytes |
| `INTERVAL` | `PqInterval`, `byte[]` of 12 | `eq`, `notEq`, `in` | the 12 bytes |
| `GEOMETRY`, `GEOGRAPHY` | `byte[]`; `intersects(xmin, ymin, xmax, ymax)` | `eq`, `notEq`, `in`; `intersects` | the WKB bytes; bounding boxes |
| `NULL` | the physical type's literal | `eq`, `notEq`, `in` | the stored value |

Notes on individual rows:

- **Floating point.** `FLOAT`, `DOUBLE` and `FLOAT16` compare by `Float.compare` /
  `Double.compare` whatever `ColumnOrder` the file declares: every `NaN` equals every other and
  sorts above `+Inf`, and `-0.0` sorts below `+0.0`. The column order only says how the file's
  bounds were written.
- **`INT96`.** An `INT96` is a Julian day and a signed count of nanoseconds of that day, which the
  format does not bound to one day, so one instant has several encodings. parquet-format 2.14.0
  orders `INT96` bounds by day, then nanoseconds (`Int96TimestampOrder`), which disagrees with the
  instant on such encodings; statistics are therefore never read for an `INT96` column.
- **`TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)`.** The carrier comes from parquet-format after its
  2.14.0 release ([apache/parquet-format#601](https://github.com/apache/parquet-format/pull/601)):
  a signed 96-bit little-endian count of the unit.
- **`NULL`.** A conforming file stores only nulls there, so no comparison matches, and no order is
  defined over values that do not exist. A file that stores values anyway is compared by what it
  stores under equality, as a column exceeding an `INT(8)` annotation is.
- **`intersects`** decides row groups, not rows. It drops a row group whose bounding box does not
  overlap the query box and returns every row of the others, nulls included, so its answer depends
  on the file's bounding boxes and on whether metadata filtering is on. A query box with
  `xmin > xmax` wraps across the antimeridian, on `GEOMETRY` as on `GEOGRAPHY`: it covers `x >= xmin`
  or `x <= xmax`. A `NaN` bound is rejected when the predicate is built. It has no inverse, and
  `not` over it is rejected.

## Literals the column cannot hold

What a column can hold is set by its physical carrier, not by its annotation's value range:

| Literal | Column | Holds |
|---|---|---|
| `Instant`, `LocalDateTime` | `TIMESTAMP` over `INT64` | whole units within the `INT64` range |
| `Instant`, `LocalDateTime` | `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` | whole units |
| `Instant` | `INT96` | the Julian day range plus what an `INT64` of nanoseconds reaches beyond it |
| `LocalTime` | `TIME` | whole units |
| `LocalDate` | `DATE` | epoch days within the `INT32` range |
| `BigDecimal` | `DECIMAL` | at most `scale` fractional digits, and an unscaled value the `INT32`, `INT64` or `FIXED_LEN_BYTE_ARRAY(n)` holds; `BYTE_ARRAY` holds any |
| `byte[]` | fixed-width column | exactly the width |
| `float` | `FLOAT16` | a value a half represents (`NaN` included) |
| `PqInterval` | `INTERVAL` | components within `[0, 2^32 − 1]` |

- **Equality** (`eq`, `notEq`, `in`, `not(in)`) with such a literal throws
  `IllegalArgumentException`: the question has a constant answer, and the literal is in practice a
  mistake.
- **Order** is answered exactly. With `lo(v)` the greatest held value below `v` and `hi(v)` the
  least above, `lt v` / `ltEq v` become `ltEq lo(v)` and `gt v` / `gtEq v` become `gtEq hi(v)`
  (floor and ceiling division for time units, `RoundingMode.FLOOR` / `CEILING` at the scale). Where
  `lo(v)` or `hi(v)` does not exist, the literal lies past the carrier's range and the predicate
  matches every non-null row or none. A comparison that is exact on the literal as given, such as a
  byte string of another width or a `float` against a `FLOAT16`, compares as given.
- **Annotation ranges** do not bound a literal: `eq(i8, 1000)` on an `INT(8)` column, a `DECIMAL`
  literal past the precision and a `TIME` of 25 hours are compared as given. `getInt` returns what
  a file stores even past its annotation, and the predicate follows it. (`getValue` on a signed
  `INT(8)` or `INT(16)` narrows such a value, so the two accessors disagree on those files.)

## Byte and string literals

A `byte[]` literal is the stored bytes on every binary column. Where a type has several encodings
of one value, the two literals part:

| Column | Several encodings | `byte[]` matches | Typed literal matches |
|---|---|---|---|
| `DECIMAL` over `BYTE_ARRAY` | `00 02`, `02` (sign-extension padding), empty for zero | that encoding | every encoding |
| `FLOAT16` | `NaN` with any sign or payload | that encoding | every `NaN` |
| `INT96` | nanoseconds of the day past one day | that encoding | every encoding |

Because a `byte[]` predicate depends on the bytes alone, recognising a new annotation can turn an
ordered `byte[]` predicate into a refusal, never into a different answer.

A `String` is the literal of the columns `getString` reads: `STRING`, `ENUM`, `JSON` and unannotated
`BYTE_ARRAY`, whose bytes are its UTF-8 encoding. Elsewhere it is rejected: `"1.25"` against a
`DECIMAL` means a number, not the bytes `31 2E 32 35`. A `String` that is not well-formed UTF-16 has
no UTF-8 encoding and is rejected when built.

A `String` matches the rows whose bytes are its UTF-8 encoding. Where a text column stores bytes
that are not well-formed UTF-8, as an unannotated `BYTE_ARRAY` of binary data does, `getString`
replaces each malformed sequence with U+FFFD, and that string does not match the row it was read
from. Such a row is filtered with its `byte[]`.

The factories are the supported way to build a predicate; the records are public only because the
sealed `FilterPredicate` permits them. `byte[]` factories copy their array, and array-valued records
compare by content.

## Nulls and composition

A comparison never matches a null (SQL three-valued logic): `notEq(x, 5)` does not return rows
where `x` is null, and `not(p)` keeps them out, so `not(gt(x, v))` and `ltEq(x, v)` agree on every
row. `isNull` / `isNotNull` test nulls explicitly. `and`, `or` and `not` compose freely; `not` is
lowered to the leaves by inverting each operator.

## Errors

At reader creation, a predicate the rule does not admit throws `IllegalArgumentException` naming
the column and what it takes, e.g. `Column 'c' is annotated DATE, which takes LocalDate and int
literals, not a BigDecimal`. That covers a literal type the column does not take, an equality
literal it cannot hold, an ordered operator on an unordered type or with a `byte[]` on a
value-ordered one, a `String` off a text column, a column below a repeated path, a comparison on a
`VARIANT` leaf, and `not` over `intersects`. An ordered operator on a type with no order is refused
before the literal's type is looked at, and the message names the literals the column takes with
`eq`, `notEq` and `in`.

When the predicate is built, a null column name, literal or child throws `NullPointerException`
naming the argument, and an empty set form, an `and` or `or` without children, a malformed `String`
and a `NaN` bound of `intersects` throw `IllegalArgumentException`.

## Resolution

`FilterPredicateResolver` turns each leaf into a `ResolvedPredicate` over the column's physical
type:

- **Typed literals** convert to the carrier: `LocalDate` to epoch days, `Instant` / `LocalDateTime`
  to the unit, `BigDecimal` to the unscaled value at the scale, rounding as described above. A
  literal past the carrier's range resolves to `NoRowPredicate` on one side and a comparison with
  the carrier's extreme on the other; `ResolvedPredicate.negate` turns `NoRowPredicate` into
  `EveryNonNullRowPredicate`.
- **`BOOLEAN`** ordered operators resolve to an equality or a constant, so every evaluator answers a
  boolean column through one comparison.
- **Set forms** resolve probe by probe into the physical type's membership test; `not(in)` becomes
  the conjunction of `notEq`.
- **`String`** is admitted where `TextColumns.holdsText` says `getString` reads, then resolves as its
  UTF-8 `byte[]`.
- **`byte[]`** goes through `orderingLiteral`, an exhaustive switch over `LogicalType` that names the
  typed literal of a value-ordered column, so a new annotation has to declare its order. A
  byte-ordered column resolves to `Comparison.BYTE_STRING`. On a value-ordered one the ordered
  operators throw and equality resolves as below:

  | Column | `byte[]` equality |
  |---|---|
  | `DECIMAL` over `FIXED_LEN_BYTE_ARRAY` | `FIXED_DECIMAL` (one encoding per number, so byte equality) |
  | `DECIMAL` over `BYTE_ARRAY` | `VARIABLE_DECIMAL` and `STORED_BYTES` |
  | `FLOAT16` | `Float16Predicate` of the decoded half and `STORED_BYTES` |
  | `INT96` | `STORED_BYTES` |

  `STORED_BYTES` reads no bounds; the value comparison beside it prunes on the value-ordered
  bounds, which is sound because a row storing those bytes holds that value.
- **Dictionary and Bloom filter** shortcuts test exact bytes, so they apply only to comparisons
  with one encoding per value (`Comparison.byteExact`): every `byte[]`, but not a `BigDecimal` on
  `BYTE_ARRAY`, a `float` on `FLOAT16` or an `Instant` on `INT96`.
- **Refusal messages** name the column's literals through `ColumnLiterals`, another exhaustive
  switch over `LogicalType`.
- **`parquet-java-compat`**'s `FilterConverter` reads the file schema and turns a parquet-java
  `Binary` on a `DECIMAL` into a `BigDecimal` predicate and a 2-byte one on a `FLOAT16` into a
  `float` predicate, as parquet-java compares them; every other `Binary` becomes a `byte[]`.

## Relation to parquet-java and DuckDB

On conforming files from mainstream writers, with the typed literal of each column, Hardwood,
parquet-java 1.17.1 and DuckDB 1.4.4 return the same rows. That covers integers, strings, dates,
millisecond and microsecond timestamps, decimals via `BigDecimal`, UUIDs, and `NaN` on files whose
writer omits the bounds of a chunk holding `NaN`. The differences below were measured with
`tools/predicate-audit` (see Validation) on fixtures written by parquet-java and PyArrow, each read
by all three engines, and fall into three groups:

- **Hardwood follows its rule where another engine chose differently.** These are
  semantics, so a migrated query can silently return different rows.
- **Another engine has a defect.** Hardwood is right, and the difference disappears when the
  engine is fixed.
- **Only unusual or non-conforming files** show the difference.

The **Relevance** column estimates how often a real query meets the difference.

| # | Data | Predicate | Hardwood | parquet-java | DuckDB | Cause | Relevance |
|---|---|---|---|---|---|---|---|
| 1 | `INT32 i`: `20, 22, null` | `notEq(i, 20)` | `22` | `22, null` | `22` | Rule: three-valued logic | **High** for code migrated from parquet-java: nulls under `notEq` / `notIn` |
| 2 | `INT32 i`: `20, 22, 24, null` | `not(in(i, 20, 22))` | `24` | all 4 rows | `24` | parquet-java defect: `notIn` of more than one value | Medium, parquet-java users |
| 3 | `DOUBLE d`, PyArrow: row group `1.0, 2.0, NaN, 3.0` with bounds `[1.0, 3.0]` | `gt(d, 5.0)`, `eq(d, NaN)` | includes `NaN` | row group dropped | row group dropped | Engine defects: pruning ignores `NaN` outside the bounds | Medium on float data with `NaN` (Arrow writers) |
| 4 | `DECIMAL(9, 2)` over `INT32`: `1.25` | `eq(dec, 125)` | `1.25` | `1.25` | none (`125.00`) | Rule: `int` is the unscaled value | Medium; `BigDecimal` is the natural literal |
| 5 | same | `eq(dec, 1.255)` | throws | no spelling | none | Rule 4: equality literal the column cannot hold | Low; fails loudly |
| 6 | `TIMESTAMP(NANOS)`, PyArrow: `t`, `t+1ns` … `t+7ns` | `gt(ts, t+4ns)` | `t+5ns` … `t+7ns` | same | none (`eq` returns all) | DuckDB defect: reads nanoseconds as microseconds | Low; nanosecond-exact filters only |
| 7 | `DOUBLE d`: `-0.0, +0.0` | `eq(d, 0.0)` | `+0.0` | `+0.0` | both | Rule: `Double.compare`, as parquet-java | Low |
| 8 | `UINT_32 u`: `0, 4000000000` | `lt(u, -1)` | both (`-1` is `0xFFFFFFFF`) | both | none | Rule: bit-pattern literal, as parquet-java | Low; unsigned columns are rare on the JVM |
| 9 | `FLOAT16 f`: `NaN` stored as `00 7E` and `00 FE` | `eq(f, new byte[] {0x00, 0x7E})` | `00 7E` only | both | no spelling | Rule 2: `byte[]` is the stored bytes; the shim converts parquet-java's `Binary` to `float` and agrees with it | Low |
| 10 | `INTERVAL iv`: 310 months, 0 days, 310 s | `eq(iv, new PqInterval(0, 9300, 310_000))` | none | no spelling | the row (normalised) | Rule: `INTERVAL` defines no conversion between components | Low |
| 11 | `INT96 ts` | `gt(ts, …)` | by instant | by bytes as a signed big-endian integer | by instant | parquet-java's comparator; the shim refuses an ordered `Binary` | Low |
| 12 | `DECIMAL(30, 3)` over `BYTE_ARRAY` storing `0.002` as `00 02`, Bloom filter | `Binary` / `byte[]` `02` | none | the row, except where the Bloom filter drops it | no spelling | parquet-java defect: Bloom and dictionary test bytes, rows test value | Low; needs a padded encoding |
| 13 | `INT32` annotated `INT(8)` storing `1000` | `eq(i8, 1000)` | the row | the row | none (reads `-24`) | Non-conforming file; Hardwood follows `getInt` | Low |
| 14 | any column, through `parquet-java-compat` | `FilterApi.eq(intColumn("x"), null)` | throws | the null rows | — | The shim supports no null literal; use `isNull` | Medium for shim users; fails loudly |
| 15 | `DECIMAL(9, 2)` over `FIXED_LEN_BYTE_ARRAY(9)` storing `0.20` | `Binary` / `byte[]` `14` | throws | the row, except where the dictionary or Bloom filter drops it | no spelling | Rule 4, and the parquet-java defect of row 12 | Low; needs a literal of another width |
| 16 | `DECIMAL(30, 3)` over `BYTE_ARRAY` storing `0` as no bytes, Bloom filter | `eq(dec, BigDecimal.ZERO)` | the row | none | the row | parquet-java defect: the Bloom filter tests the literal's bytes `00` | Low; needs an empty encoding |
| 17 | `INTERVAL` written by parquet-java (`converted_type = INTERVAL`, `logicalType = UNKNOWN`) | `eq(iv, PqInterval)` | throws | the row | none (read as `INTEGER`) | Hardwood defect [#1217](https://github.com/hardwood-hq/hardwood/issues/1217): the column is read as `NULL` | Medium for `INTERVAL` data from parquet-java |

In short: code moving from parquet-java should check negations over nullable columns (1). Code
comparing with DuckDB should use `BigDecimal` for decimals (4), and should expect Hardwood to
return `NaN` rows that DuckDB drops (3). The other rows need rare types, literals or files.

## Validation

- **`PredicatePathAgreementTest`** runs the per-column table, except `GEOGRAPHY` (whose literals
  are `GEOMETRY`'s), with every literal
  kind, operator, `not` form, set form and null test except `intersects`. It checks against an
  oracle of this rule that reads each row's value through the reader's accessors.
  - **Paths:** the `RowReader` by default, forced onto its record-level path, and with
    `hardwood.metadata-filtering=false`; the `ColumnReader` with and without metadata filtering.
  - **Layouts:** one row group, several row groups, dictionary-encoded, and several row groups with
    Bloom filters, so that bounds, page index, dictionary and Bloom filter each decide a predicate
    again. Also a struct leaf that is null under a present struct, and a corpus of `BSON`,
    `INTERVAL`, `NULL`, `GEOMETRY` and a padded and empty `BYTE_ARRAY` `DECIMAL`, kept apart because
    DuckDB cannot open a file holding `BSON`. Then `INT96` columns with a non-canonical encoding
    and bounds recorded in byte order, and `TIMESTAMP` columns over `FIXED_LEN_BYTE_ARRAY(12)` of
    every unit, past the `INT64` nanosecond range on both sides of the epoch.
- **`FilterPredicateResolverTest`**: every literal kind of the table, accepted with the expected
  resolved predicate or rejected with the full message.
- **`FilterPredicateTest`**: build-time checks, copies and content equality of `byte[]` literals.
- **`ParquetReaderCompatTest`**: `Binary` literals on `DECIMAL` and `FLOAT16` answer as parquet-java.
- **`tools/predicate-audit`**, run by hand when the rule, pruning or a compared engine changes; CI
  compiles it but does not run it:
  - **Matrix:** about 72,000 predicate cells through the same five paths, checked against a second
    oracle of this rule that takes stored values from the fixture generator rather than from the
    reader. parquet-java writes the fixtures in the same four layouts: the per-column table, the
    `BSON`, `NULL` and `GEOMETRY` columns, a low-cardinality copy that is dictionary-encoded
    throughout, and nested structs with a `LIST`. Footer rewrites derive converted-type-only and
    dropped-annotation variants from the per-column table, and annotate the `TIMESTAMP` columns over
    `FIXED_LEN_BYTE_ARRAY(12)` that parquet-java writes as plain bytes, with bounds in the order of
    their values.
  - **Resolver matrix:** every literal kind and operator against every column and group, including
    `VARIANT` groups written by a footer rewrite.
  - **Consultation checks:** Bloom filters (against a copy with zeroed bitsets) and dictionaries are
    shown to be read where the rule allows it and left unread where it does not.
  - **Engine comparison:** the parquet-java and DuckDB comparison behind the table above, including
    a PyArrow file for rows 3, 6 and 7, except for row 14 (the compatibility shim), which
    `ParquetReaderCompatTest` covers.
  - **Accessor round-trip:** every value an accessor returns, passed back as an `eq` literal,
    matches its own row, except a `String` read from bytes that are not UTF-8.

  `tools/predicate-audit/README.md` describes running and extending it.
