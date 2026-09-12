# Predicate literals

**Status: Proposed** — scope by issue below.

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
   nor a `double` for a `FLOAT` column.
2. **A literal matches a row when it denotes the value the row holds, and an ordered predicate
   compares in the order the column's type defines.** Both sides are values of the column's type.
   A byte literal stands for what the column's annotation decodes it to — a half for `FLOAT16`, a
   number for `DECIMAL`, an instant for `INT96` — and so does the stored value, whichever of its
   literals a predicate is given.
3. **Ordered operators exist exactly on types that define an order.**
4. **Every literal type with `eq` has a set form**, `BOOLEAN` excepted.
5. **An equality literal is a value the column can hold; an order literal is any value of the
   literal type.**
6. **`String` is the literal where `getString` reads the column.**

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
| `FLOAT16` | `float`; `byte[]` of two bytes | all | the half the value encodes, as `Float.compare` |
| `DATE` | `LocalDate`, `int` | all | epoch day |
| `TIME` | `LocalTime`, `int` (millis) / `long` (micros, nanos) | all | the column's unit |
| `TIMESTAMP` over `INT64`, `isAdjustedToUTC = true` | `Instant`, `long` | all | the column's unit |
| `TIMESTAMP` over `INT64`, `isAdjustedToUTC = false` | `LocalDateTime`, `long` | all | the wall clock in the column's unit |
| `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)`, `isAdjustedToUTC = true` | `Instant`; `byte[]` of twelve bytes | all | the signed 96-bit count of the column's unit |
| `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)`, `isAdjustedToUTC = false` | `LocalDateTime`; `byte[]` of twelve bytes | all | the signed 96-bit wall-clock count of the column's unit |
| `INT96` | `Instant`; `byte[]` of twelve bytes | all | the instant the value encodes |
| `DECIMAL` over `INT32` / `INT64` | `BigDecimal`, `int` / `long` unscaled | all | the represented value |
| `DECIMAL` over `BYTE_ARRAY` | `BigDecimal`, `byte[]` | all | the represented value, any encoding length |
| `DECIMAL` over `FIXED_LEN_BYTE_ARRAY` | `BigDecimal`, `byte[]` | all | the represented value; a `byte[]` is sign-extended or trimmed to the width |
| `STRING`, `ENUM`, `JSON` | `String`, `byte[]` | all | unsigned lexicographic |
| unannotated `BYTE_ARRAY` | `String`, `byte[]` | all | unsigned lexicographic |
| unannotated `FIXED_LEN_BYTE_ARRAY` | `byte[]` | all | unsigned lexicographic |
| `BSON` | `byte[]` | all | unsigned lexicographic |
| `UUID` | `UUID`, `byte[]` | all | unsigned lexicographic over the 16 bytes |
| `INTERVAL` | `PqInterval`, `byte[]` | equality and set form | the 12 bytes |
| `GEOMETRY`, `GEOGRAPHY` | `byte[]`; `intersects(xmin, ymin, xmax, ymax)` | equality and set form; `intersects` | the WKB bytes; `intersects` by bounding box |
| `NULL` | the physical type's literal | equality and set form | no value matches |

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
the nanoseconds stay within one day, so statistics are not used to prune an `INT96` column.

`intersects` has no inverse: `not` over a predicate containing `intersects` is rejected.

## Literals the column cannot hold

A literal can be a value of the column's literal type that the column cannot hold. What a column
can hold is set by its physical carrier:

| Literal | Column | The column holds |
|---|---|---|
| `Instant`, `LocalDateTime` | `TIMESTAMP` over `INT64` | a whole number of the column's unit within the `INT64` range |
| `Instant`, `LocalDateTime` | `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` | a whole number of the column's unit |
| `Instant` | `INT96` | an instant whose Julian day fits a signed 32-bit integer |
| `LocalTime` | `TIME` | a whole number of the column's unit |
| `LocalDate` | `DATE` | an epoch day within the `INT32` range |
| `BigDecimal` | `DECIMAL` | no more fractional digits than the scale, and an unscaled value within the `INT32` / `INT64` range, or within the width of a `FIXED_LEN_BYTE_ARRAY` after sign extension; a `BYTE_ARRAY` holds any unscaled value |
| `byte[]` | `FIXED_LEN_BYTE_ARRAY`, `UUID`, `INTERVAL` | exactly the width; for a fixed-width `DECIMAL`, any length whose value fits the width after sign extension |
| `byte[]` | `BYTE_ARRAY` | any length |
| `float` | `FLOAT16` | a value a half represents |
| `PqInterval` | `INTERVAL` | each component within `[0, 2^32 − 1]` |

An annotation's value range does not bound a literal. That covers the bit width of an `INT(8)`,
the precision of a `DECIMAL` and the single day of a `TIME`. The accessors return what a file
stores, and a file may exceed its annotation. A literal outside that range is compared as given
and matches no row of a file that keeps to its annotation.

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

For `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY` the physical literal is `byte[]`: the stored bytes,
compared in the column's order.

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

The dictionary and Bloom filter shortcuts for equality test exact bytes. They apply only where a
value has one encoding in the column (`Comparison.byteExact`). They do not apply to a `DECIMAL`
over `BYTE_ARRAY`, whose writer may pad, nor to `INT96`, whose nanoseconds-of-day field can
express a value past one day.

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
- a `String` where `getString` does not read
- a column below a repeated path, or a comparison on a leaf below a `VARIANT` group
- `not` over `intersects`

Two errors are raised when the predicate is built. Every factory rejects a null literal with a
`NullPointerException` naming the argument, and a `String` that is not well-formed UTF-16 throws
`IllegalArgumentException`.

## Resolution

`FilterPredicateResolver` turns each leaf into the internal predicate of the column's physical
type. A typed literal is converted at resolution: `LocalDate` to epoch days, `Instant` /
`LocalDateTime` to the column's unit, `BigDecimal` to the unscaled value at the column's scale.
Where the conversion is not exact, it rounds as "Literals the column cannot hold" describes. A
literal past the column's range resolves to the "no row" constant on the side whose bound is
missing, and to a comparison against the carrier's extreme on the other. `ResolvedPredicate.negate`
maps that constant to "every non-null row", which a membership test also negates to when no probe
is a value the column holds.

A `StringColumnPredicate` / `StringInPredicate` resolves in two steps. First, `requireTextColumn`
decides whether the column takes a `String`. It is an exhaustive `switch` over `LogicalType`, the
shape `byteComparison` has, so an annotation added later has to state whether it takes one; an
unannotated column takes one only as a `BYTE_ARRAY`. Second, the value is encoded as UTF-8 and
resolved as the equivalent `byte[]` literal. Every column that passes the first step compares as
`BYTE_STRING`.

A `BinaryColumnPredicate` / `BinaryInPredicate` resolves through `byteComparison` and
`comparedBytes`, which carry the `FLOAT16` decoding, the fixed-width `DECIMAL` resize, and the
`byteExact` gating of the dictionary and Bloom filter shortcuts. `parquet-java-compat`'s
`FilterConverter` turns a parquet-java `Binary` literal into a `BinaryColumnPredicate`, so a
migrated filter compares in the column's order on every binary column.

## Relation to parquet-java and DuckDB

The rule follows from the accessors, not from either engine. Most predicates that all three can
express get the same answer; the differences are listed below.

- **parquet-java.** Its `filter2` API takes the physical literal only (`int`, `long`, `float`,
  `double`, `boolean`, `Binary`) and compares it through the column's `PrimitiveComparator`, which
  the annotation selects: `UNSIGNED_INT32_COMPARATOR` for `UINT_32`,
  `BINARY_AS_SIGNED_INTEGER_COMPARATOR` for `DECIMAL`, `BINARY_AS_FLOAT16_COMPARATOR` for
  `FLOAT16`. Its floating-point comparators order as `Float.compare`, as here. It also accepts
  ordered predicates on types without an order, offers equality and membership only on
  `BOOLEAN`, and orders `INT96` by its bytes as a signed big-endian integer rather than as the
  instant it holds.
- **DuckDB.** It takes the literal of the column's SQL type only, which the annotation determines.
  An integer against a `DATE` and a `BLOB` against a `DECIMAL` are conversion errors, and a string
  is parsed as text of the column's type (`dec = '1.25'`). The typed literals here give the same
  answers, with three differences:
  - DuckDB treats `-0.0` and `+0.0` as equal.
  - An integer against a `DECIMAL` is the number there, where an `int` literal here is the
    unscaled value.
  - An unsigned column's literal is the unsigned value there and the stored bit pattern here, as
    in parquet-java.
- **Strings.** On a text column or an unannotated `BYTE_ARRAY`, a `String` is its UTF-8 bytes in
  all three. On a typed binary column DuckDB parses it as text and parquet-java has no such
  literal; here it is rejected, as on an unannotated `FIXED_LEN_BYTE_ARRAY`, which no accessor
  reads as a `String`.

## Scope by issue

| Part | Issue |
|---|---|
| Fixed-width `DECIMAL` byte literal resolved to the column width | [#1190](https://github.com/hardwood-hq/hardwood/issues/1190) (done) |
| `byte[]` literals and factories; `String` only where `getString` reads; `String` records; null literals and malformed `String`s rejected when built | [#1181](https://github.com/hardwood-hq/hardwood/issues/1181) (done) |
| Ordered operators exactly on ordered types: `BOOLEAN` gains them, with record-level and batch matchers; `INTERVAL`, `GEOMETRY`, `GEOGRAPHY` and `NULL` lose them. `PqInterval` literal; leaves below a `VARIANT` group take null tests only; `not` over `intersects` rejected | [#1183](https://github.com/hardwood-hq/hardwood/issues/1183) |
| `INT96` literals | [#1192](https://github.com/hardwood-hq/hardwood/issues/1192) |
| Literals the column cannot hold; the constant predicates | [#1193](https://github.com/hardwood-hq/hardwood/issues/1193) (done) |
| `LocalDateTime` literal; `Instant` on UTC timestamps only | [#1194](https://github.com/hardwood-hq/hardwood/issues/1194) |
| Set forms for every literal type; `in(double...)` on `DOUBLE` only; `in(String, String...)` | [#1195](https://github.com/hardwood-hq/hardwood/issues/1195), [#1178](https://github.com/hardwood-hq/hardwood/issues/1178) |
| `getString` reads text columns only | [#1196](https://github.com/hardwood-hq/hardwood/issues/1196) |
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
    `BSON` column.
- `FilterPredicateResolverTest` covers every row of the per-column table for every literal kind and
  operator: accepted with the expected resolved predicate, or rejected with the full message. For
  every resolved leaf form, `not(not(p))` resolves to a predicate equivalent to `p`.
- `FilterPredicateTest`: the `byte[]` factories copy their literal and compare by content, every
  set form rejects an empty list, and every factory rejects a null literal.
- `DifferentialColumnOrderTest`: byte literals go through the `byte[]` factories.
- `ParquetReaderCompatTest`: the `binaryColumn` cases on a `DECIMAL` column stay green.
