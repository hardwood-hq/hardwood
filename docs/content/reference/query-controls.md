<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Query Controls

Look-it-up reference for the predicate and projection controls. For worked examples and the I/O
behavior of each control — predicate pushdown, projection, row limits, splits, and skip — see
[Predicate Pushdown, Projection, Limits, and Splits](../how-to/query-controls.md).

## Supported filter predicates

| Category | Supported |
|---|---|
| Comparison operators | `eq`, `notEq` on every column; `lt`, `ltEq`, `gt`, `gtEq` on a column whose type defines an order |
| Set operators | `in`, over every literal type but `boolean` |
| Null operators | `isNull`, `isNotNull` (any type) |
| Spatial operators | `intersects`, on a `GEOMETRY` or `GEOGRAPHY` column. It has no inverse, so `not` over a predicate holding one throws `IllegalArgumentException` at reader creation |
| Combinators | `and`, `or`, `not` (`and` / `or` accept varargs for three or more conditions) |
| Column form | By name or dot-separated path (`address.city`). Comparison predicates take leaf columns only; `isNull` / `isNotNull` also take the name of a group — a struct, a `LIST` or a `MAP`. Any name below a repeated path is rejected |

All predicates, including those wrapped in `not`, are pushed down for row-group and page
skipping. Filters work with all reader types — `RowReader`, `ColumnReader`,
`AvroRowReader`, and across multi-file readers.

## Predicate literals by column type

A column takes the literal types listed for its physical type, and — where it carries an
annotation — those listed for that annotation as well. A `String` is the one literal that does
not compose this way: it filters a `STRING`, an `ENUM`, a `JSON` and an unannotated `BYTE_ARRAY`,
and no other binary column. Where both rows apply, the annotation names the order: a `UINT_32`
column matches the `INT32` row and the `INT(32, isSigned = false)` row, and compares by unsigned
magnitude. Set membership follows the same mapping: `in` takes each literal type below but
`boolean`, and each of its values is an equality literal. An empty set throws
`IllegalArgumentException` when the predicate is built. A literal a column does not take throws
`IllegalArgumentException` at reader creation.

```java
FilterPredicate filter = FilterPredicate.in("status", "ACTIVE", "PENDING");
FilterPredicate filter = FilterPredicate.in("ratio", 0.25f, 0.5f);
FilterPredicate filter = FilterPredicate.in("placed_at",
        Instant.parse("2025-01-01T00:00:00Z"), Instant.parse("2025-07-01T00:00:00Z"));
```

`eq`, `notEq` and the set form are available on every column below. The ordered operators `lt`,
`ltEq`, `gt` and `gtEq` are available on the types that define an order, which is every one except
`INTERVAL`, `GEOMETRY`, `GEOGRAPHY` and `NULL`; on those four they throw `IllegalArgumentException`
at reader creation. On a `DECIMAL`, `FLOAT16` or `INT96` column they take the typed literal, and a
`byte[]` literal takes `eq`, `notEq` and the set form only (see [Binary columns](#binary-columns)).
`BOOLEAN` is the one type with no set form, since `eq`, `notEq` and `isNotNull` express every set
of two values.

| Physical type | Logical type | Literal | Compared as |
|---|---|---|---|
| `BOOLEAN` | | `boolean` | `false` before `true` |
| `INT32` | | `int` | signed |
| `INT64` | | `long` | signed |
| `FLOAT` | | `float` | numeric |
| `DOUBLE` | | `double` | numeric |
| `BYTE_ARRAY` | | `byte[]`; `String` where the column carries no annotation | unsigned lexicographic |
| `FIXED_LEN_BYTE_ARRAY(n)` | | `byte[]` of `n` bytes | unsigned lexicographic |
| `INT32` 8/16/32-bit, `INT64` 64-bit | `INT(8/16/32/64, isSigned = true)` | `int` / `long` | signed |
| `INT32` 8/16/32-bit, `INT64` 64-bit | `INT(8/16/32/64, isSigned = false)` | `int` / `long` | unsigned magnitude |
| `INT32` | `DATE` | `LocalDate` | days since the Unix epoch |
| `INT32` millis, `INT64` micros / nanos | `TIME` | `LocalTime` | the column's time unit |
| `INT64` | `TIMESTAMP(isAdjustedToUTC = true)`, and the legacy `TIMESTAMP_MILLIS` / `TIMESTAMP_MICROS` | `Instant` | the column's time unit |
| `INT64` | `TIMESTAMP(isAdjustedToUTC = false)` | `LocalDateTime` | the wall clock, in the column's time unit |
| `INT96` | | `Instant`, `byte[]` of 12 bytes | the instant the value encodes; a `byte[]` as the stored bytes |
| `INT32` up to 9 digits, `INT64` up to 18 | `DECIMAL` | `BigDecimal`; `int` / `long` unscaled | the represented value |
| `FIXED_LEN_BYTE_ARRAY(n)` up to what `n` bytes hold, `BYTE_ARRAY` any | `DECIMAL` | `BigDecimal`; `byte[]`, of `n` bytes on a `FIXED_LEN_BYTE_ARRAY(n)` | the represented value; a `byte[]` as the stored bytes |
| `BYTE_ARRAY` | `STRING`, `ENUM`, `JSON` | `String`, `byte[]` | unsigned lexicographic |
| `BYTE_ARRAY` | `BSON` | `byte[]` | unsigned lexicographic |
| `BYTE_ARRAY` | `GEOMETRY`, `GEOGRAPHY` | `byte[]`; four `double` bounds for `intersects` | the WKB bytes, unsigned; `intersects` by bounding-box overlap |
| `FIXED_LEN_BYTE_ARRAY(16)` | `UUID` | `UUID`, `byte[]` of 16 bytes | the 16 bytes, unsigned |
| `FIXED_LEN_BYTE_ARRAY(2)` | `FLOAT16` | `float`, `byte[]` of 2 bytes | numeric, widened to `float`; a `byte[]` as the stored bytes |
| `FIXED_LEN_BYTE_ARRAY(12)` | `INTERVAL` | `PqInterval`, `byte[]` of 12 bytes | the 12 bytes |
| any | `NULL` | the literal for the physical type | nothing — every value is null, so no comparison matches |
| group of two `BYTE_ARRAY` | `VARIANT` | `isNull`, `isNotNull` | whether the group is present |

A predicate on a `VARIANT` column reaches the group's presence, not the values inside it. The
`metadata` and `value` leaves below the group, and a shredded variant's `typed_value` leaves, hold
the encoded variant, which [`getVariant`](accessors.md) reads off the group; they take `isNull` and
`isNotNull` only, and any other predicate on one throws `IllegalArgumentException` at reader
creation. Filtering on a shredded variant's sub-paths is in progress, tracked by
[#309](https://github.com/hardwood-hq/hardwood/issues/309).

A `FLOAT` column compares by the `Float.compare` total order and a `DOUBLE` column by
`Double.compare`, so all `NaN` values equal each other and `-0.0` differs from `+0.0`.

A `BigDecimal` literal is rescaled to the column's scale before it is compared. A column with
more scale than the literal pads it, so `99.99` against a `DECIMAL(scale = 4)` column compares
as `99.9900`, and trailing zeros drop the same way. A literal carrying a digit the column's
scale cannot hold follows the rule under
[Literals the column cannot hold](#literals-the-column-cannot-hold).

An `INT96` column is a legacy timestamp, which [`getTimestamp`](accessors.md) reads as an
`Instant`. An `Instant` literal compares as the instant the value encodes. The format lets one
instant be stored under more than one encoding, since the nanoseconds of the day are not bounded by
one day, and every encoding of an instant matches an `Instant` literal of it. The column also takes
the 12 stored bytes that `getBinary` returns, which match the rows storing exactly those bytes; a
`byte[]` literal of any other length throws `IllegalArgumentException` at reader creation.

```java
FilterPredicate filter = FilterPredicate.gtEq("event_time", Instant.parse("2015-06-01T00:00:00Z"));
```

An unsigned column's literal is the stored two's-complement bit pattern, the same form
[the accessors](accessors.md) hand back for it: `4_000_000_000` in a `UINT_32` column is the `int`
`-294_967_296`, so a value read from a row can be passed straight back as a predicate literal.
Write one with `Integer.parseUnsignedInt` / `Long.parseUnsignedLong`. Comparisons order by the
unsigned magnitude regardless, so that literal is above every positive `int` rather than below
zero.

A column that carries an annotation also takes the literal for its physical type, comparing the
value as it is stored: an `int` against a `DATE` column tests the epoch day directly.

### Binary columns

The literal of a `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` column is a `byte[]`, the stored bytes
that [`getBinary`](accessors.md) returns. The factories copy the array, so a caller reusing it
does not change a predicate already built.

```java
FilterPredicate filter = FilterPredicate.eq("code", new byte[] { 0x00, (byte) 0xC8 });
FilterPredicate members = FilterPredicate.in("code",
        new byte[] { 0x00, (byte) 0xC8 }, new byte[] { 0x00, (byte) 0xC9 });
```

A `byte[]` literal matches the rows storing exactly its bytes, on every binary column. On most
columns the values order as their bytes, unsigned, and a `byte[]` takes every operator. A
`DECIMAL`, a `FLOAT16` and an `INT96` order by the value their bytes encode, and there a `byte[]`
takes `eq`, `notEq` and `in` only; `lt`, `ltEq`, `gt` and `gtEq` with a `byte[]` throw
`IllegalArgumentException` at reader creation, naming the `BigDecimal`, `float` or `Instant`
literal that takes them.

| Column | `byte[]` literal | Matches |
|---|---|---|
| `DECIMAL` over `BYTE_ARRAY` | any length | the rows storing these bytes; a padded encoding of the same number is a different literal |
| `DECIMAL` over `FIXED_LEN_BYTE_ARRAY(n)` | `n` bytes | the rows storing these bytes, which is the one encoding of the number |
| `FLOAT16` | 2 bytes | the rows storing these bytes; each `NaN` encoding is a different literal |
| `INT96` | 12 bytes | the rows storing these bytes; another encoding of the same instant is a different literal |

A `byte[]` literal of another length than the table gives throws `IllegalArgumentException` at
reader creation.

A `String` literal is the literal of a text column — `STRING`, `ENUM`, `JSON` and an unannotated
`BYTE_ARRAY` — where its UTF-8 encoding is exactly the stored bytes. These are the columns
[`getString`](accessors.md#text-columns) reads. On any other binary column a `String`
literal throws `IllegalArgumentException` at reader creation, naming the literals the column
does take. A `String` that is not well-formed UTF-16, such as one holding an unpaired surrogate, has
no UTF-8 encoding and throws `IllegalArgumentException` when the predicate is built.

An `INTERVAL` column's other literal is the `PqInterval` that [`getInterval`](accessors.md)
returns. Each of its three components is stored as an unsigned 32-bit value.

```java
FilterPredicate filter = FilterPredicate.eq("uptime", new PqInterval(0, 1, 3_600_000));
```

Every factory rejects a null literal with a `NullPointerException` naming the argument.

## Literals the column cannot hold

A literal can be a value of the column's literal type that the column itself cannot store: finer
than the time unit of a `TIMESTAMP` or `TIME`, carrying more decimal places than a `DECIMAL`'s
scale, past the range of the `INT32` or `INT64` that holds the column's values, of a width a
fixed-width column does not have, a `float` no IEEE half represents, a `PqInterval` with a
component outside `[0, 4294967295]`, or an `Instant` later or earlier than every instant an
`INT96` column encodes.

Equality (`eq`, `notEq`, a set form and its negation) asks whether a stored value *is* the
literal. Against a value the column cannot store, `eq` could never match and `notEq` always
would, so it throws `IllegalArgumentException` when the reader is built, naming the column, what
bounds it and the literal:

```java
// Column 'ts' holds a whole number of microseconds within the INT64 range;
// the equality literal 2024-05-01T12:00:00.000000500Z is not a value it can hold
reader.buildRowReader()
        .filter(FilterPredicate.eq("ts", Instant.parse("2024-05-01T12:00:00.000000500Z")))
        .build();
```

An ordered predicate (`lt`, `ltEq`, `gt`, `gtEq`) asks instead where the literal sits in the
column's order, which every value of the literal type has an answer for, and is answered exactly.
`lt("ts", Instant.parse("2024-05-01T12:00:00.000000500Z"))` on a microsecond column returns every
row up to and including `12:00:00.000000`. Where the literal lies past the range the column's
carrier holds, the predicate matches every non-null row or none: `lt("d", LocalDate.MAX)` on a
`DATE` column returns every non-null row, and `gt("d", LocalDate.MAX)` returns none. A comparison
never matches a null row, and `not` over one of these does not either.

On a fixed-width column whose bytes compare as a byte string, a byte literal of another width
compares as given for an ordered predicate, since the comparison is exact on it either way. On a
fixed-width `DECIMAL`, an ordered predicate against a `BigDecimal` past the column's width matches
every non-null row or none: against a literal above the largest number the width holds, `lt` and
`ltEq` return every non-null row and `gt` and `gtEq` none, and against one below the smallest, the
other way round.

An annotation's value range does not bound a literal. That covers the bit width of an `INT(8)`,
the precision of a `DECIMAL` and the single day of a `TIME`: [the physical
accessors](accessors.md#physical-accessors) `getInt` and `getLong` return what a file stores, so
`eq("i8", 1000)` on an `INT(8)` column is compared as given and matches no row of a file that keeps
to its annotation.

## When statistics are ignored

Pruning compares a unit's `min` / `max` bounds. A pair a reader cannot compare against is
ignored rather than trusted, so the row group or page is kept and its rows are read and filtered
one by one. Results are the same either way; only the I/O saved is lost. Bounds are ignored for
one of seven reasons:

| Reason | Bounds |
|---|---|
| The minimum sorts above the maximum | `min` and `max` are the wrong way round in the column's order, so the pair brackets nothing |
| One of them is `NaN` | A `FLOAT`, `DOUBLE` or `FLOAT16` bound. `TYPE_ORDER` forbids it; under `IEEE_754_TOTAL_ORDER` it marks a unit whose every non-null value is `NaN` |
| They come from the deprecated `min` / `max` fields | Superseded by `min_value` / `max_value`; the deprecated pair compares unsigned whatever the column's type is, so its order is wrong for every signed one |
| The column's annotation defines no order | An `INTERVAL`, `GEOMETRY`, `GEOGRAPHY`, `VARIANT`, `UNKNOWN`, `LIST` or `MAP` column, for which the Parquet spec defines no sort order |
| The column is `INT96` | The order the Parquet spec gives `INT96` bounds compares the day before the nanoseconds of the day, which does not follow the instant when the nanoseconds run past one day |
| The column's annotation is dropped | An annotation the column's physical type cannot carry, or one this release does not recognize, so the column is read as its physical type while its bounds were recorded in the annotation's order |
| The file declares a `ColumnOrder` this release does not recognize | The Parquet spec directs a reader to ignore `min` / `max` under a column order it does not support; this applies to every column type |

The bounds themselves are still reported as the file carries them, by `Statistics` on the metadata
API and by `hardwood inspect` and `hardwood dive`.

Usable floating-point bounds cover a unit's non-`NaN` values only. A predicate that a `NaN`
value satisfies — `notEq`, `gt` or `gtEq` against a number, or `eq`, `ltEq` or `gtEq` against
`NaN` — prunes a `FLOAT`, `DOUBLE` or `FLOAT16` row group or page from its bounds only where the
unit records a `nan_count` of zero. `eq`, `lt` and `ltEq` against a number, and `gt` against
`NaN`, prune from the bounds alone.

## Column projection forms

| Form | Description |
|------|-------------|
| `ColumnProjection.all()` | Read all columns (default) |
| `ColumnProjection.columns("id", "name")` | Read specific columns by name |
| `ColumnProjection.columns("address")` | Select an entire struct and all its children |
| `ColumnProjection.columns("address.city")` | Select a specific nested field (dot notation) |
