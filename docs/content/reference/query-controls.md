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
| Comparison operators | `eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq` |
| Set operators | `in` (int, long, double), `inStrings` |
| Null operators | `isNull`, `isNotNull` (any type) |
| Spatial operators | `intersects`, on a `GEOMETRY` or `GEOGRAPHY` column |
| Combinators | `and`, `or`, `not` (`and` / `or` accept varargs for three or more conditions) |
| Column form | By name or dot-separated path (`address.city`). Comparison predicates take leaf columns only; `isNull` / `isNotNull` also take the name of a group — a struct, a `LIST` or a `MAP`. Any name below a repeated path is rejected |

All predicates, including those wrapped in `not`, are pushed down for row-group and page
skipping. Filters work with all reader types — `RowReader`, `ColumnReader`,
`AvroRowReader`, and across multi-file readers.

## Predicate literals by column type

A column takes the literal types listed for its physical type, and — where it carries an
annotation — those listed for that annotation as well. Where both rows apply, the annotation
names the order: a `UINT_32` column matches the `INT32` row and the `INT(32, isSigned = false)`
row, and compares by unsigned magnitude. Set membership follows the same mapping: `in` on the
`INT32`, `INT64`, `FLOAT`, `DOUBLE` and `FLOAT16` columns, `inStrings` on those taking a `String`. A
literal a column does not take throws `IllegalArgumentException` at reader creation.

| Physical type | Logical type | Literal | Compared as |
|---|---|---|---|
| `BOOLEAN` | | `boolean` | equality only (`eq`, `notEq`) |
| `INT32` | | `int` | signed |
| `INT64` | | `long` | signed |
| `FLOAT` | | `float` | numeric |
| `DOUBLE` | | `double` | numeric |
| `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY(n)` | | `String` | unsigned lexicographic |
| `INT32`, `INT64` | `INT(8/16/32/64, isSigned = true)` | `int` / `long` | signed |
| `INT32`, `INT64` | `INT(8/16/32/64, isSigned = false)` | `int` / `long` | unsigned magnitude |
| `INT32` | `DATE` | `LocalDate` | days since the Unix epoch |
| `INT32` millis, `INT64` micros / nanos | `TIME` | `LocalTime` | the column's time unit |
| `INT64` | `TIMESTAMP` | `Instant` | the column's time unit |
| `INT32`, `INT64`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` | `DECIMAL` | `BigDecimal`, `String` | the represented value, under all four physical types |
| `BYTE_ARRAY` | `STRING`, `ENUM`, `JSON`, `BSON` | `String` | unsigned lexicographic |
| `BYTE_ARRAY` | `GEOMETRY`, `GEOGRAPHY` | four `double` bounds | bounding-box overlap |
| `FIXED_LEN_BYTE_ARRAY(16)` | `UUID` | `UUID`, `String` | the 16 bytes, unsigned |
| `FIXED_LEN_BYTE_ARRAY(2)` | `FLOAT16` | `float`, `String` | numeric, widened to `float` |
| `FIXED_LEN_BYTE_ARRAY(12)` | `INTERVAL` | `String`, `inStrings` | the 12 bytes, unsigned; the format defines no order for `INTERVAL`, so only equality is meaningful |
| any | `NULL` | the literal for the physical type | nothing — every value is null, so no comparison matches |
| group of two `BYTE_ARRAY` | `VARIANT` | `isNull`, `isNotNull` | whether the group is present |

A predicate on a `VARIANT` column reaches the group's presence, not the values inside it.
Filtering on a shredded variant's sub-paths is in progress, tracked by
[#309](https://github.com/hardwood-hq/hardwood/issues/309); the `metadata` and `value` leaves below
the group take `BYTE_ARRAY` predicates as any leaf does, but they hold the encoded payload rather
than the values a caller would filter on.

A `FLOAT` or `DOUBLE` column compares by the `Double.compare` total order, so all `NaN` values
equal each other and `-0.0` differs from `+0.0`. A `FLOAT` column's stored values widen to
`double` first, so a probe with no exact `float` representation — `0.1`, say — never matches.

A `BigDecimal` literal is rescaled to the column's scale before it is compared. A column with
more scale than the literal pads it, so `99.99` against a `DECIMAL(scale = 4)` column compares
as `99.9900`, and trailing zeros drop the same way. A literal carrying a digit the column's
scale cannot hold throws `ArithmeticException` rather than rounding it away — `99.999` against
a `DECIMAL(scale = 2)` column.

An unsigned column's literal is the stored two's-complement bit pattern, the same form
[the accessors](accessors.md) hand back for it: `4_000_000_000` in a `UINT_32` column is the `int`
`-294_967_296`, so a value read from a row can be passed straight back as a predicate literal.
Write one with `Integer.parseUnsignedInt` / `Long.parseUnsignedLong`. Comparisons order by the
unsigned magnitude regardless, so that literal is above every positive `int` rather than below
zero.

A column that carries an annotation also takes the literal for its physical type, comparing the
value as it is stored: an `int` against a `DATE` column tests the epoch day directly.

`DECIMAL` and `FLOAT16` are the exceptions to *how* the bytes compare. Both order by the value
their bytes stand for rather than by the bytes themselves, and their statistics are written in
that order, so a `String` literal against either compares as the column does — a `DECIMAL` by its
unscaled value, a `FLOAT16` by the number its two little-endian bytes encode — rather than as a
byte string. A `FLOAT16` literal must be exactly two bytes. On a `FIXED_LEN_BYTE_ARRAY` `DECIMAL`,
a literal of any length stands for the value it encodes and is brought to the column width:
sign-extended when it is shorter, its leading sign-extension bytes dropped when it is longer. One
whose value needs more bytes than the column holds throws `ArithmeticException`.

`inStrings` compares each probe the same way, so on a `DECIMAL` a padded encoding of a probe is
still a member, and on a `FLOAT16` each probe — exactly two bytes — is compared as the half it
encodes. `in(double...)` on a `FLOAT16` compares against the decoded half as it does against a
`FLOAT`'s widened value, so a probe no half represents, such as `0.1`, matches nothing.

Note that a `String` literal is encoded as UTF-8, which reproduces a byte one-for-one only below
`0x80`. A `DECIMAL`'s unscaled value sets the high bit for every negative number, so those are
not expressible this way; reach for the `BigDecimal` factory instead.

## When statistics are ignored

Pruning compares a unit's `min` / `max` bounds. A pair a reader cannot compare against is
ignored rather than trusted, so the row group or page is kept and its rows are read and filtered
one by one. Results are the same either way; only the I/O saved is lost. Bounds are ignored for
one of five reasons:

| Reason | Bounds |
|---|---|
| The minimum sorts above the maximum | `min` and `max` are the wrong way round in the column's order, so the pair brackets nothing |
| One of them is `NaN` | A `FLOAT`, `DOUBLE` or `FLOAT16` bound. `TYPE_ORDER` forbids it; under `IEEE_754_TOTAL_ORDER` it marks a unit whose every non-null value is `NaN` |
| They come from the deprecated `min` / `max` fields | Superseded by `min_value` / `max_value`; the deprecated pair compares unsigned whatever the column's type is, so its order is wrong for every signed one |
| The column's annotation defines no order | An `INTERVAL`, `GEOMETRY`, `GEOGRAPHY`, `VARIANT`, `UNKNOWN`, `LIST` or `MAP` column, for which the Parquet spec defines no sort order |
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
