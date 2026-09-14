# Predicate literals

**Status: Implemented**, except the `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` rows, which come with
[#921](https://github.com/hardwood-hq/hardwood/issues/921). Scope by issue below.

Which predicates a column takes, with which literals, and what they mean.

## Rule

1. **A literal is a value an accessor returns for the column.** A column takes exactly the values
   its documented accessors return (`reference/accessors.md`). Those are the value of its logical
   accessor and the value of its physical accessor:
   - logical: `getDate` → `LocalDate`, `getTimestamp` → `Instant`, `getLocalTimestamp` →
     `LocalDateTime`, `getTime` → `LocalTime`, `getDecimal` → `BigDecimal`, `getUuid` → `UUID`,
     `getInterval` → `PqInterval`, `getFloat` on `FLOAT16` → `float`, `getString` → `String`
   - physical: `boolean`, `int`, `long`, `float`, `double`, or `byte[]` through `getBinary`

   Literal types do not widen into each other: an `int` is not a literal for an `INT64` column,
   nor a `double` for a `FLOAT` column. `String` is the literal where `getString` reads the
   column. Every column takes each of its literals for equality and, `BOOLEAN` excepted, the set
   form.
2. **A literal matches a row when it denotes the value the row holds.** A typed literal denotes a
   value of the column's type. An `int` or `long` denotes the stored integer, which on an
   unsigned `INT` column is the bit pattern. A `byte[]` literal denotes the stored bytes: it
   matches a row storing exactly those bytes, whatever the column's annotation reads them as.
3. **A literal takes the ordered operators where it compares in the order the column's type
   defines.** A typed literal compares by the value it denotes. An `int` or `long` compares signed,
   and unsigned on an unsigned `INT` column. A `byte[]` compares its bytes unsigned
   lexicographically, which is the order of every binary type but a `DECIMAL`, a `FLOAT16`, an
   `INT96` and a `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)`; those order by the value their bytes
   encode and take a `byte[]` for equality and the set form only. A type that defines no order
   takes no ordered operator.
4. **An equality literal is a value the column can hold; an order literal is any value of the
   literal type.**

## What can be filtered

- A leaf column, at the top level or inside structs. A leaf below a repeated path — inside a
  `LIST` or a `MAP` — occurs many times per row and has no single answer. A predicate on it is
  rejected.
- A group — struct, `LIST`, `MAP`, `VARIANT` — through `isNull` / `isNotNull`, testing whether the
  group itself is present. No accessor reads the leaves below a `VARIANT` group (`metadata`,
  `value`, and shredded `typed_value` leaves). They take `isNull` / `isNotNull` only.
- A column carrying only a legacy `converted_type` is promoted to its logical type and follows
  that type's row. A column whose annotation its physical type cannot carry, or one not
  recognized, is read as its physical type and follows that row.

## Per column type

"All operators" means `eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq`, the set form and its negation.
Every column also takes `isNull` / `isNotNull`.

| Column | Literals | Operators | Compared as |
|---|---|---|---|
| `BOOLEAN` | `boolean` | all but the set form | `false` < `true` |
| `INT32` / `INT64` | `int` / `long` | all | signed |
| `INT(8/16/32/64)`, signed | `int` / `long` | all | signed |
| `INT(8/16/32/64)`, unsigned | `int` / `long`, the stored bit pattern | all | unsigned magnitude |
| `FLOAT` | `float` | all | `Float.compare` |
| `DOUBLE` | `double` | all | `Double.compare` |
| `FLOAT16` | `float`; `byte[]` of two bytes | all; `byte[]` equality and set form | the half the value encodes, as `Float.compare`; a `byte[]` as the stored bytes |
| `DATE` | `LocalDate`, `int` | all | epoch day |
| `TIME` | `LocalTime`, `int` (millis) / `long` (micros, nanos) | all | the column's unit |
| `TIMESTAMP` over `INT64`, `isAdjustedToUTC = true` | `Instant`, `long` | all | the column's unit |
| `TIMESTAMP` over `INT64`, `isAdjustedToUTC = false` | `LocalDateTime`, `long` | all | the wall clock in the column's unit |
| `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)`, `isAdjustedToUTC = true` | `Instant`; `byte[]` of twelve bytes | all; `byte[]` equality and set form | the signed 96-bit count of the column's unit; a `byte[]` as the stored bytes |
| `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)`, `isAdjustedToUTC = false` | `LocalDateTime`; `byte[]` of twelve bytes | all; `byte[]` equality and set form | the signed 96-bit wall-clock count of the column's unit; a `byte[]` as the stored bytes |
| `INT96` | `Instant`; `byte[]` of twelve bytes | all; `byte[]` equality and set form | the instant the value encodes; a `byte[]` as the stored bytes |
| `DECIMAL` over `INT32` / `INT64` | `BigDecimal`, `int` / `long` unscaled | all | the represented value |
| `DECIMAL` over `BYTE_ARRAY` | `BigDecimal`, `byte[]` | all; `byte[]` equality and set form | the represented value, any encoding length; a `byte[]` as the stored bytes |
| `DECIMAL` over `FIXED_LEN_BYTE_ARRAY` | `BigDecimal`; `byte[]` of the column width | all; `byte[]` equality and set form | the represented value; a `byte[]` as the stored bytes |
| `STRING`, `ENUM`, `JSON` | `String`, `byte[]` | all | unsigned lexicographic |
| unannotated `BYTE_ARRAY` | `String`, `byte[]` | all | unsigned lexicographic |
| unannotated `FIXED_LEN_BYTE_ARRAY` | `byte[]` | all | unsigned lexicographic |
| `BSON` | `byte[]` | all | unsigned lexicographic |
| `UUID` | `UUID`, `byte[]` | all | unsigned lexicographic over the 16 bytes |
| `INTERVAL` | `PqInterval`, `byte[]` | equality and set form | the 12 bytes |
| `GEOMETRY`, `GEOGRAPHY` | `byte[]`; `intersects(xmin, ymin, xmax, ymax)` | equality and set form; `intersects` | the WKB bytes; `intersects` by bounding box |
| `NULL` | the physical type's literal | equality and set form | the stored value, which a conforming file never has |

`FLOAT`, `DOUBLE` and `FLOAT16` values compare by `Float.compare` / `Double.compare`: every `NaN`
equals every other `NaN` and sorts above `+Inf`, and `-0.0` sorts below `+0.0`. This holds
whatever `ColumnOrder` the file declares for the column; the column order says only how the
file's bounds were written, and pruning reads them accordingly.

## Operators

The ordered operators (`lt`, `ltEq`, `gt`, `gtEq`) are available exactly on the types that define
an order. `BOOLEAN` orders `false` before `true`. `INTERVAL`, `GEOMETRY`, `GEOGRAPHY` and `NULL`
define none and take equality and the set form only.

`INT96` values are instants and order as such. The format's `Int96TimestampOrder` orders an
`INT96` by its day and then its nanoseconds of the day. That agrees with the instant only while
the nanoseconds stay within one day. Under `TYPE_ORDER` the format leaves the order of an
`INT96` undefined. Statistics are therefore never read for an `INT96` column, and ignoring them
is not reported as a discarded pair.

A stored value's nanoseconds of the day are not bounded by one day, so an `INT96` holds instants
past its first and last Julian day, up to the extreme day plus what an `INT64` of nanoseconds
reaches. That range is what the column can hold, for equality and order alike. A literal past
the extreme day keeps that day and carries the remaining days in its nanoseconds, which compares
as the same instant.

`intersects` has no inverse: `not` over a predicate containing `intersects` is rejected.

`intersects` decides row groups, not rows. It drops a row group whose bounding box does not overlap
the query box and returns every row of the others, nulls included, so its answer depends on the
bounding boxes the file records and on whether metadata filtering is on. It is the one leaf whose
answer differs between the read paths.

A `NULL` column stores only nulls in a conforming file, so no comparison matches a row of one. A
file that stores values in such a column anyway is compared by the value it stores, as a column
storing values past an `INT(8)` annotation is.

## Literals the column cannot hold

A literal can be a value of the column's literal type that the column cannot hold. What a column
can hold is set by its physical carrier:

| Literal | Column | The column holds |
|---|---|---|
| `Instant`, `LocalDateTime` | `TIMESTAMP` over `INT64` | a whole number of the column's unit within the `INT64` range |
| `Instant`, `LocalDateTime` | `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` | a whole number of the column's unit |
| `Instant` | `INT96` | an instant within its extreme Julian days plus what an `INT64` of nanoseconds reaches |
| `LocalTime` | `TIME` | a whole number of the column's unit |
| `LocalDate` | `DATE` | an epoch day within the `INT32` range |
| `BigDecimal` | `DECIMAL` | no more fractional digits than the scale, and an unscaled value within the `INT32` / `INT64` range, or within the width of a `FIXED_LEN_BYTE_ARRAY` after sign extension; a `BYTE_ARRAY` holds any unscaled value |
| `byte[]` | `FIXED_LEN_BYTE_ARRAY`, `UUID`, `INTERVAL`, `DECIMAL` over `FIXED_LEN_BYTE_ARRAY` | exactly the width |
| `byte[]` | `BYTE_ARRAY` | any length |
| `float` | `FLOAT16` | a value a half represents |
| `PqInterval` | `INTERVAL` | each component within `[0, 2^32 − 1]` |

An annotation's value range does not bound a literal. That covers the bit width of an `INT(8)`,
the precision of a `DECIMAL` and the single day of a `TIME`. The physical accessors (`getInt`,
`getLong`) return what a file stores, and a file may exceed its annotation. A literal outside that
range is compared as given and matches no row of a file that keeps to its annotation.

For an annotated integer the rule follows the physical accessor. `getInt` returns a value stored
past an `INT(8)` or `INT(16)` annotation as stored, and a predicate compares that value. On a
signed `INT(8)` or `INT(16)` the generic `getValue` returns the annotation's `Byte` or `Short`,
keeping the low 8 or 16 bits of such a value (a stored `1000` reads as `-24`), so a row of a file
that exceeds its annotation reads differently through the two. On an unsigned one `getValue`
returns the stored `Integer`.

- An equality literal (`eq`, `notEq`, a set form, its negation) the column cannot hold throws
  `IllegalArgumentException`.
- An order literal the column cannot hold is answered exactly. Let `lo(v)` be the greatest value
  the column holds below `v` and `hi(v)` the least above it. Then `lt v` and `ltEq v` resolve to
  `ltEq lo(v)`, and `gt v` and `gtEq v` to `gtEq hi(v)`. The rounding is exact for the unit:
  floor and ceiling division for a time unit, `RoundingMode.FLOOR` / `CEILING` at the scale for a
  `BigDecimal`. Where the comparison is exact on the literal as given it compares as given, as
  with a byte string of another width or a `float` against a `FLOAT16`.
- Past the column's range, one of `lo(v)` and `hi(v)` does not exist. The operator that needs the
  missing one matches no row. The other still has its bound, the extreme value the carrier holds,
  and so matches every non-null row. Neither matches a null row, so `not` over either keeps nulls
  out.

A byte literal that does not decode to a value of the column's type at all is a type error. That
means a `FLOAT16` literal other than two bytes, and an `INT96` or `FIXED_LEN_BYTE_ARRAY(12)`
`TIMESTAMP` literal other than twelve.

## Set forms

```java
in(String column, int... values)            in(String column, LocalDate... values)
in(String column, long... values)           in(String column, Instant... values)
in(String column, float... values)          in(String column, LocalDateTime... values)
in(String column, double... values)         in(String column, LocalTime... values)
in(String column, String... values)         in(String column, BigDecimal... values)
in(String column, byte[]... values)         in(String column, UUID... values)
                                            in(String column, PqInterval... values)
```

Each takes the columns its literal type takes, and each value follows the rules above as an
equality literal. An empty list throws `IllegalArgumentException`. `BOOLEAN` has no set form: with
two values, `eq`, `notEq` and `isNotNull` express every set. `inStrings(String, String...)` is a
deprecated spelling of `in(String, String...)`.

## Binary literals

For `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` and `INT96` the physical literal is `byte[]`: the stored
bytes. Equality and the set form match the rows storing exactly those bytes. The ordered operators
compare them unsigned, on the types whose order that is, and reject them on the others (rule 3),
naming the typed literal that orders the column.

The bytes and the value part where a type admits more than one encoding of a value: a
`BYTE_ARRAY` `DECIMAL` padded with sign-extension bytes, a `FLOAT16` `NaN` with another sign or
payload, an `INT96` whose nanoseconds of the day run past one day. A `byte[]` literal matches its
own encoding only; the typed literal matches every encoding. A `byte[]` predicate's result depends
on the bytes alone, so recognising an annotation the reader does not know today can turn an
answered ordered `byte[]` predicate into a refusal, but never into a different result.

`String` is the literal where `getString` reads the column: `STRING`, `ENUM`, `JSON` and an
unannotated `BYTE_ARRAY`. Its UTF-8 encoding is exactly the stored bytes. A `String` that is not
well-formed UTF-16, such as one holding an unpaired surrogate, has no UTF-8 encoding and is
rejected when the predicate is built.

A `String` is not a spelling of bytes on any other column. UTF-8 reproduces a byte one-for-one
only below `0x80`, and a caller writing `"1.25"` against a `DECIMAL` means the number, not the
bytes `31 2E 32 35`.

```java
static FilterPredicate eq(String column, byte[] value);   // … notEq, lt, ltEq, gt, gtEq
static FilterPredicate in(String column, byte[]... values);
static FilterPredicate eq(String column, String value);   // … notEq, lt, ltEq, gt, gtEq
static FilterPredicate in(String column, String... values);
```

The `byte[]` factories copy the literal, so a caller reusing its array does not change a
predicate already built. The factories are the supported way to build a predicate; the records
are public only because the sealed `FilterPredicate` permits them.

| Record | Built by | Literal |
|---|---|---|
| `BinaryColumnPredicate(String column, Operator op, byte[] value)` | `byte[]` comparison factories, `parquet-java-compat` | bytes |
| `BinaryInPredicate(String column, byte[][] values)` | `in(String, byte[]...)` | bytes |
| `StringColumnPredicate(String column, Operator op, String value)` | `String` comparison factories | text |
| `StringInPredicate(String column, String[] values)` | `in(String, String...)`, `inStrings` | text |

The array-valued records compare by content in `equals` / `hashCode` and copy their arrays on
construction.

The dictionary and Bloom filter shortcuts for equality test exact bytes. They apply to every
`byte[]` literal, and to a typed literal only where a value has one encoding in the column
(`Comparison.byteExact`). They do not apply to a `BigDecimal` on a `DECIMAL` over `BYTE_ARRAY`,
whose writer may pad, nor to an `Instant` on an `INT96`, whose nanoseconds-of-day field can express
a value past one day.

## Nulls and composition

A comparison never matches a null value: comparing a null against any literal is unknown, and
rows whose predicate is unknown are not returned (SQL three-valued logic). `not(p)` keeps them
unknown, so `not(gt(x, v))` and `ltEq(x, v)` agree on every row. `isNull` / `isNotNull` test nulls
explicitly. `and`, `or` and `not` compose freely, and `not` is lowered to the leaves at resolution
by inverting each operator. `intersects` is the one leaf without an inverse.

## Errors

A predicate the rule does not admit throws `IllegalArgumentException` at reader creation, naming
the column and what it takes. That covers:

- a literal type the column does not take
- an equality literal it cannot hold
- a byte literal that does not decode
- an ordered operator on a type without an order
- an ordered operator with a `byte[]` on a type that does not order as its bytes
- a `String` where `getString` does not read
- a column below a repeated path, or a comparison on a leaf below a `VARIANT` group
- `not` over `intersects`

Three errors are raised when the predicate is built. Every factory rejects a null literal with a
`NullPointerException` naming the argument, every set form rejects an empty list with an
`IllegalArgumentException`, and a `String` that is not well-formed UTF-16 throws
`IllegalArgumentException`.

## Resolution

`FilterPredicateResolver` turns each leaf into the internal predicate of the column's physical
type. A typed literal is converted at resolution: `LocalDate` to epoch days, `Instant` /
`LocalDateTime` to the column's unit, `BigDecimal` to the unscaled value at the column's scale.
Where the conversion is not exact, it rounds as "Literals the column cannot hold" describes. A
literal past the column's range resolves to the "no row" constant on the side whose bound is
missing, and to a comparison against the carrier's extreme on the other. `ResolvedPredicate.negate`
maps that constant to "every non-null row". A set form resolves probe by probe as the equality
literal of the same type does, into the membership test of the column's physical type, and its
negation is the conjunction of `notEq` over the probes. A `BOOLEAN` column holds two values and
nothing between them, so each ordered operator on one resolves the same way: to an equality against
`false` or `true`, or to a constant. Every evaluator therefore answers a boolean column through one
comparison.

A `StringColumnPredicate` / `StringInPredicate` resolves in two steps. First, `requireTextColumn`
decides whether the column takes a `String`, through `TextColumns`, which `getString` asks as well.
Its `nonTextLiterals` is an exhaustive `switch` over `LogicalType`, the shape `orderingLiteral` has,
so an annotation added later has to state whether it takes one; an unannotated column takes one
only as a `BYTE_ARRAY`. Second, the value is encoded as UTF-8 and
resolved as the equivalent `byte[]` literal. Every column that passes the first step compares as
`BYTE_STRING`.

A `BinaryColumnPredicate` / `BinaryInPredicate` resolves through `orderingLiteral`, an exhaustive
`switch` over `LogicalType` naming the typed literal of a column that does not order as its bytes,
so an annotation added later has to state whether it does. A column that orders as its bytes
resolves to `Comparison.BYTE_STRING`. On the others the ordered operators throw, and equality
resolves as follows, `notEq` as its negation:

| Column | Equality resolves to |
|---|---|
| `DECIMAL` over `FIXED_LEN_BYTE_ARRAY` | `FIXED_DECIMAL`, which one encoding per number makes byte equality |
| `DECIMAL` over `BYTE_ARRAY` | `VARIABLE_DECIMAL` and `STORED_BYTES` |
| `FLOAT16` | the `Float16Predicate` of the decoded half and `STORED_BYTES` |
| `INT96` | `STORED_BYTES` |

`STORED_BYTES` compares the bytes and reads no bounds. The value comparison beside it reads the
bounds in the order they are written in, which prunes soundly: a row storing the literal's bytes
holds the value they encode. `parquet-java-compat`'s `FilterConverter` reads the file schema and
turns a parquet-java `Binary` literal on a `DECIMAL` over a binary type into a
`DecimalColumnPredicate`, and one of two bytes on a `FLOAT16` into a `FloatColumnPredicate`, which
compare as parquet-java's comparators do. Every other `Binary` becomes a `BinaryColumnPredicate`.

## Relation to parquet-java and DuckDB

The rule follows from the accessors, not from either engine. Most predicates that all three can
express get the same answer; the differences are listed below.

- **parquet-java.** Its `filter2` API takes the physical literal only (`int`, `long`, `float`,
  `double`, `boolean`, `Binary`) and compares it through the column's `PrimitiveComparator`, which
  the annotation selects: `UNSIGNED_INT32_COMPARATOR` for `UINT_32`,
  `BINARY_AS_SIGNED_INTEGER_COMPARATOR` for `DECIMAL`, `BINARY_AS_FLOAT16_COMPARATOR` for
  `FLOAT16`. A `Binary` there therefore compares as the value on a `DECIMAL` and a `FLOAT16`, which
  here is the typed literal's meaning, and the compatibility shim converts it accordingly. Its
  floating-point comparators order as `Float.compare`, as here. The differences:
  - `notEq` and `notIn` match null rows; here a comparison never does. In 1.17.1 a `notIn` of
    more than one value matches every row, its members included.
  - It accepts ordered predicates on types without an order, and offers equality and membership
    only on `BOOLEAN`.
  - It orders `INT96` by its bytes as a signed big-endian integer rather than as the instant it
    holds. The shim passes a `Binary` on an `INT96` through as a `byte[]`, so an ordered one throws.
  - Its dictionary and Bloom filter checks compare a `Binary` by its bytes while its record-level
    comparison is by value, so on a `DECIMAL` a `Binary` of another encoding than the stored one
    matches in some layouts and not in others. Here a `byte[]` is the stored bytes on every path.
  - Its statistics pruning (1.17.1) drops `NaN` rows from a row group whose bounds exclude `NaN`.
- **DuckDB.** It takes the literal of the column's SQL type only, which the annotation determines.
  An integer against a `DATE` and a `BLOB` against a `DECIMAL` are conversion errors, and a string
  is parsed as text of the column's type (`dec = '1.25'`). The typed literals here give the same
  answers, with these differences:
  - DuckDB treats `-0.0` and `+0.0` as equal.
  - An integer against a `DECIMAL` is the number there, where an `int` literal here is the
    unscaled value.
  - An unsigned column's literal is the unsigned value there and the stored bit pattern here, as
    in parquet-java.
  - An `INT(8)` or `INT(16)` value stored past its annotation is narrowed there, as `getValue`
    narrows it; here a predicate compares the value `getInt` returns.
  - An `INTERVAL` compares normalised there, so 9,300 days equals 310 months; here equality is
    the 12 stored bytes.
  - An equality literal the column cannot hold, such as a `DECIMAL` literal past the scale,
    matches no row there and throws here.
  - A `TIMESTAMP(NANOS)` or `TIME(NANOS)` column is read at microsecond precision there, so
    literals a nanosecond apart compare equal.
  - Its statistics pruning drops `NaN` rows from a row group whose bounds exclude `NaN`, as
    parquet-java's does.
- **Strings.** On a text column or an unannotated `BYTE_ARRAY`, a `String` is its UTF-8 bytes in
  all three. On a typed binary column DuckDB parses it as text and parquet-java has no such
  literal; here it is rejected, as on an unannotated `FIXED_LEN_BYTE_ARRAY`, which no accessor
  reads as a `String`.

## Scope by issue

| Part | Issue |
|---|---|
| Fixed-width `DECIMAL` byte literal resolved to the column width | [#1190](https://github.com/hardwood-hq/hardwood/issues/1190) (done) |
| `byte[]` literals and factories; `String` only where `getString` reads; `String` records; null literals and malformed `String`s rejected when built | [#1181](https://github.com/hardwood-hq/hardwood/issues/1181) (done) |
| Ordered operators exactly on ordered types: `BOOLEAN` gains them; `INTERVAL`, `GEOMETRY`, `GEOGRAPHY` and `NULL` lose them. `PqInterval` literal; leaves below a `VARIANT` group take null tests only; `not` over `intersects` rejected | [#1183](https://github.com/hardwood-hq/hardwood/issues/1183) (done) |
| `INT96` literals | [#1192](https://github.com/hardwood-hq/hardwood/issues/1192) (done) |
| Literals the column cannot hold; the constant predicates | [#1193](https://github.com/hardwood-hq/hardwood/issues/1193) (done) |
| `LocalDateTime` literal; `Instant` on UTC timestamps only | [#1194](https://github.com/hardwood-hq/hardwood/issues/1194) (done) |
| Set forms for every literal type; `in(double...)` on `DOUBLE` only; `in(String, String...)` | [#1195](https://github.com/hardwood-hq/hardwood/issues/1195), [#1178](https://github.com/hardwood-hq/hardwood/issues/1178) (done) |
| `getString` reads text columns only | [#1196](https://github.com/hardwood-hq/hardwood/issues/1196) (done) |
| `getValue` narrows a stored signed `INT(8)` / `INT(16)` value past the annotation; the rule follows `getInt` | [#1203](https://github.com/hardwood-hq/hardwood/issues/1203) (done) |
| A column whose annotation is dropped has no readable bounds | [#1139](https://github.com/hardwood-hq/hardwood/issues/1139) (done) |
| `byte[]` is the stored bytes; ordered `byte[]` only on types that order as their bytes; `FilterConverter` maps `Binary` on `DECIMAL` / `FLOAT16` to typed literals | [#1142](https://github.com/hardwood-hq/hardwood/issues/1142) (done) |
| `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` | [#921](https://github.com/hardwood-hq/hardwood/issues/921) |

The floating-point rows depend on NaN-aware pruning
([#1016](https://github.com/hardwood-hq/hardwood/issues/1016)). The column-reader answers depend on
the column-reader record view handling `FLOAT16` and struct leaves
([#1197](https://github.com/hardwood-hq/hardwood/issues/1197)).

## Validation

- `PredicatePathAgreementTest` runs every row of the per-column table against a Java oracle of the
  rule above. It covers every literal kind, operator, `not` form, set form and null test.
  - **Paths:** the `RowReader` by default and forced onto the record-level path, the `RowReader`
    with `hardwood.metadata-filtering=false`, and the `ColumnReader` with and without metadata
    filtering.
  - **Layouts:** single row group, multiple row groups, and dictionary-encoded, so that the
    row-group bounds, the page index and the dictionary each decide a predicate the record-level
    comparison decides again; a struct whose leaf is null under a present struct; and the `BSON`
    and `INTERVAL` columns, in a corpus of their own because DuckDB cannot open a file holding a
    `BSON` column; and `INT96` columns in the three flat layouts, one value stored under a
    non-canonical encoding and bounds recorded in the byte order of a big-endian integer, so that
    both the exact-byte shortcuts and pruning would drop a row that matches.
- `FilterPredicateResolverTest` covers the literal kinds of the per-column table: accepted with the
  expected resolved predicate, or rejected with the full message. A negated set form resolves to the
  conjunction of `notEq` over its probes.
- `FilterPredicateTest`: the `byte[]` factories copy their literal and compare by content, every
  set form rejects an empty list, and every factory rejects a null literal.
- `DifferentialColumnOrderTest`: byte equality and membership go through the `byte[]` factories.
- `ParquetReaderCompatTest`: `binaryColumn` equality and order on a `DECIMAL` over either binary
  type, with literals narrower and wider than a fixed-width column, and order on a `FLOAT16`
  answer as parquet-java does; a literal the shim does not convert is refused by the reader.
