# Predicate model

This document covers the public `FilterPredicate`, the rule that decides which predicates a column takes and which rows they match, the resolution into the internal `ResolvedPredicate` tree that every evaluator reads, and the translation of parquet-java `filter2` predicates in `parquet-java-compat`. How metadata decides whether a row group or page can match is in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md), and how the readers evaluate a resolved predicate row by row is in [RECORD_FILTERING.md](RECORD_FILTERING.md). The user-facing statement of the rule, with its examples and per-type prose, is [reference/query-controls.md](../docs/content/reference/query-controls.md).

## Surface

`dev.hardwood.reader.FilterPredicate` is a sealed interface. Its static factories are the only supported way to build one:

| Kind | Factories |
|---|---|
| Comparison | `eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq`, one overload per literal type; `PqInterval` has `eq` and `notEq` only |
| Set | `in(column, values...)`, one overload per literal type except `boolean`; the negated set is `not(in(...))` |
| Null test | `isNull`, `isNotNull` |
| Spatial | `intersects(column, xmin, ymin, xmax, ymax)` |
| Combinator | `and`, `or` (two children or varargs), `not` |

The records that implement the interface (`IntColumnPredicate`, `DecimalInPredicate`, `And`, `Not`, ...) are public only because a sealed interface's permitted subtypes nested in it are. Their constructors are not supported API, and a predicate shape that no factory builds is a gap in the factories. Validation therefore lives in the factories: they apply the schema-independent checks below, and the resolver applies the literal rule against the file's schema. A record built directly bypasses the factory checks. Modules in this repository may build records directly (`FilterConverter` in `parquet-java-compat` does, since it maps an operator value) and validate their input before they do.

A reader takes a predicate through `filter(FilterPredicate)` on `ParquetFileReader`'s row reader, column reader and column readers builders, and on `AvroReaders`' row reader builder. A reader returns exactly the rows the predicate matches, in file order. The one exception is `intersects`, which decides whole row groups (see [Per column type](#per-column-type)).

### Build-time checks

The factories check what is independent of any schema:

- A null column name, literal or child throws `NullPointerException` naming the argument (`column`, `value`, `values[2]`, `filters[0]`).
- An empty set form, and an `and` or `or` without children, throw `IllegalArgumentException`.
- A `String` that is not well-formed UTF-16 throws `IllegalArgumentException`: it has no UTF-8 encoding, and encoding it would silently substitute `?`.
- A `NaN` bound of `intersects` throws `IllegalArgumentException`.

Array-valued records (`byte[]`, `int[]`, `byte[][]`, ...) copy the arrays they are built from and the arrays their accessors return, and compare by content. List-valued set records hold an immutable copy.

Tests: `FilterPredicateTest`.

## The literal rule

1. **A literal is a value an accessor returns for the column.** A column takes the value of its logical accessor (`LocalDate`, `Instant`, `LocalDateTime`, `LocalTime`, `BigDecimal`, `UUID`, `PqInterval`, a `float` on `FLOAT16`, a `String` where `getString` reads the column) and the value of its physical accessor (`boolean`, `int`, `long`, `float`, `double`, `byte[]`). Literal types do not widen: a `long` literal on a `DATE` column is refused, because no accessor returns a `long` there.
2. **A literal matches a row when it denotes the value the row holds.** A typed literal denotes a value of the column's type; an `int` or `long` denotes the stored integer (the bit pattern on an unsigned column); a `byte[]` denotes the stored bytes, whatever the annotation reads them as.
3. **A literal takes the ordered operators where it compares in the column's order.** Typed literals compare by value; `int` and `long` compare signed, or unsigned on an unsigned `INT` column; `byte[]` compares unsigned lexicographically. That is the order of every binary type except `DECIMAL`, `FLOAT16`, `INT96` and `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)`, which order by the value their bytes encode; there a `byte[]` takes equality and the set form only. A type that defines no order (`INTERVAL`, `GEOMETRY`, `GEOGRAPHY`, `NULL`) takes no ordered operator.
4. **An equality literal must be a value the column can hold; an order literal may be any value.** See [Literals the column cannot hold](#literals-the-column-cannot-hold).

Every literal type with `eq` has a set form whose values are equality literals, except `boolean`: `eq`, `notEq` and `isNotNull` already express every set of two values.

The rule makes a value read from a row usable as a literal that matches that row; the one exception is a `String` read from bytes that are not well-formed UTF-8 (see [Byte and string literals](#byte-and-string-literals)).

Tests: `tools/predicate-audit`.

### What can be filtered

- **Leaf columns**, at the top level or inside structs. A leaf below a `LIST` or `MAP` occurs many times per row and is refused for every predicate.
- **Groups** (struct, `LIST`, `MAP`, `VARIANT`) through `isNull` / `isNotNull`, which test whether the group is present. A comparison on a group is refused, since resolving it to one of the group's leaves would answer another question. The leaves below a `VARIANT` group hold the encoded variant, which no accessor reads as a value; they take `isNull` / `isNotNull` only.
- **Annotations as the reader sees them.** The rule applies to the `LogicalType` that `FileSchema` hands out after promoting legacy `converted_type` annotations and dropping annotations the physical type cannot carry or the reader does not recognize; a column whose annotation was dropped follows its physical type's row. How that annotation is derived is in [LOGICAL_TYPES.md](LOGICAL_TYPES.md).

Tests: `FilterPredicateResolverTest`.

### Per column type

"All" means `eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq`, `in` and `not(in)`. Every column also takes `isNull` / `isNotNull`. `tools/simple-datagen.py` and the audit's `flat` fixture group hold one column per row of this table.

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
| `GEOMETRY`, `GEOGRAPHY` | `byte[]`; `intersects` | `eq`, `notEq`, `in`; `intersects` | the WKB bytes; bounding boxes |
| `NULL` | the physical type's literal | `eq`, `notEq`, `in` | the stored value |

- **`INT96`** is a Julian day and a signed count of nanoseconds of that day, which the format does not bound to one day, so one instant has several encodings. An `Instant` matches every encoding of it. The order parquet-format gives `INT96` bounds (day, then nanoseconds) disagrees with the instant on such encodings, so statistics are never read for an `INT96` column ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)).
- **`TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)`** is a signed 96-bit little-endian count of the unit, from parquet-format after 2.14.0 ([apache/parquet-format#601](https://github.com/apache/parquet-format/pull/601)). The width is fixed, so each value has one encoding.
- **`NULL`.** A conforming file stores only nulls there, so no comparison matches, and no order is defined over values that do not exist. A file that stores values anyway is compared by what it stores under equality. A footer that gives `UNKNOWN` beside a `converted_type` annotates the column with the converted type, so parquet-java's `INTERVAL` columns take the `INTERVAL` row.
- **`intersects`** decides row groups, not rows. It drops a row group whose bounding box does not overlap the query box and returns every row of the others, nulls included, so its answer depends on the file's bounding boxes and on whether metadata filtering is on. A box with `xmin > xmax` wraps across the antimeridian on either type. It has no inverse, so `not` over any tree holding one is refused.

## Literals the column cannot hold

What a column can hold is set by its physical carrier, not by its annotation's value range:

| Literal | Column | Holds |
|---|---|---|
| `Instant`, `LocalDateTime` | `TIMESTAMP` over `INT64` | whole units within the `INT64` range |
| `Instant`, `LocalDateTime` | `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` | whole units within 96 bits |
| `Instant` | `INT96` | the Julian day range plus what an `INT64` of nanoseconds reaches beyond it |
| `LocalTime` | `TIME` | whole units |
| `LocalDate` | `DATE` | epoch days within the `INT32` range |
| `BigDecimal` | `DECIMAL` | at most `scale` fractional digits, and an unscaled value the `INT32`, `INT64` or `FIXED_LEN_BYTE_ARRAY(n)` holds; `BYTE_ARRAY` holds any |
| `byte[]` | fixed-width column | exactly the width |
| `float` | `FLOAT16` | a value a half represents, `NaN` included |
| `PqInterval` | `INTERVAL` | components within `[0, 2^32 − 1]` |

- **Equality** (`eq`, `notEq`, `in`, `not(in)`) with such a literal throws `IllegalArgumentException` at reader creation. The question has a constant answer, and the literal is in practice a mistake.
- **Order** is answered exactly. With `lo(v)` the greatest held value below `v` and `hi(v)` the least above, `lt v` / `ltEq v` become `ltEq lo(v)` and `gt v` / `gtEq v` become `gtEq hi(v)` (floor and ceiling division for time units, `RoundingMode.FLOOR` / `CEILING` at the scale). Where `lo(v)` or `hi(v)` does not exist, the literal lies past the carrier's range and the predicate matches every non-null row or none. A comparison that is exact on the literal as given, such as an ordered byte string of another width on an unannotated fixed-width column or a `float` against a `FLOAT16`, compares as given.
- **Annotation ranges do not bound a literal.** `eq(i8, 1000)` on an `INT(8)` column, a `DECIMAL` literal past the precision and a `TIME` of 25 hours are compared as given. `getInt` returns what a file stores even past its annotation, and the predicate follows it. (`getValue` on a signed `INT(8)` or `INT(16)` narrows such a value, so the two accessors disagree on those files.)

`CarriedLiteral` holds the pair `(lo, hi)` in `BigInteger`, so that a literal overflowing the carrier (`Instant.MAX` in nanoseconds) can be measured.

Tests: `FilterPredicateResolverTest`, `PredicatePathAgreementTest`.

## Byte and string literals

A `byte[]` literal is the stored bytes on every binary column. Where a type has several encodings of one value, the `byte[]` and the typed literal match different rows:

| Column | Several encodings | `byte[]` matches | Typed literal matches |
|---|---|---|---|
| `DECIMAL` over `BYTE_ARRAY` | `00 02` and `02` (sign-extension padding); empty for zero | that encoding | every encoding |
| `FLOAT16` | `NaN` with any sign or payload | that encoding | every `NaN` |
| `INT96` | nanoseconds of the day past one day | that encoding | every encoding |

Because a `byte[]` predicate depends on the bytes alone, recognizing a new annotation can turn an ordered `byte[]` predicate into a refusal, never into a different answer. Untested. The exhaustive `orderingLiteral` switch (see [Resolution](#resolution)) makes each new annotation declare its side.

A `String` is the literal of the columns `getString` reads: `STRING`, `ENUM`, `JSON` and unannotated `BYTE_ARRAY`, whose bytes are its UTF-8 encoding. `TextColumns.holdsText` decides this for both the accessor and the resolver, so the two cannot drift. On any other column a `String` is refused: `"1.25"` against a `DECIMAL` means a number, not the bytes `31 2E 32 35`. A `String` matches the rows whose bytes are its UTF-8 encoding. Where a text column stores bytes that are not well-formed UTF-8, `getString` replaces each malformed sequence with U+FFFD, and that string does not match the row it was read from; such a row is filtered with its `byte[]`.

## Nulls, NaN and signed zero

**Three-valued logic.** A comparison (`eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq`, `in`) never matches a null. `not(p)` keeps a row that is unknown under `p` unknown, so `not(gt(x, v))` and `ltEq(x, v)` agree on every row, null rows included, and `notEq(x, v)` returns no null row. `isNull` / `isNotNull` test nulls explicitly. The user-facing statement and the difference from parquet-java are in [how-to/query-controls.md](../docs/content/how-to/query-controls.md#null-handling) and [concepts/compatibility-philosophy.md](../docs/content/concepts/compatibility-philosophy.md#sql-three-valued-logic-for-comparison-predicates).

The rule holds because `not` is lowered to the leaves at resolution (see [Resolution](#resolution)) and every leaf evaluator answers false on a null. A `Not` node that reached an evaluator would have to carry the unknown state through; `ResolvedPredicate` has no such node.

**Floating point.** `FLOAT`, `DOUBLE` and `FLOAT16` compare by `Float.compare` / `Double.compare`: every `NaN` equals every other and sorts above `+Infinity`, and `-0.0` sorts below `+0.0`. So `eq(d, 0.0)` matches `+0.0` only, `eq(d, NaN)` matches every `NaN` payload, and `gt(d, v)` for a number `v` matches `NaN` rows.

**Column order.** The `ColumnOrder` a file declares (`TYPE_ORDER` or `IEEE_754_TOTAL_ORDER`) decides only how that file's bounds are read, never which rows match: the same values select the same rows whatever the file declares. The resolver does not read `column_orders`; its use in bounds is in [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md).

Tests: `PredicatePathAgreementTest`, `FilterPredicateResolverTest`, `NullFilterSemanticsComparisonTest` (parquet-testing-runner), `TotalOrderFloatReadTest` (parquet-testing-runner), `MixedColumnOrderPruningTest`.

## Resolution

`FilterPredicateResolver.resolve(predicate, schema)` turns the public tree into a `ResolvedPredicate` tree once per reader creation (`ParquetFileReader.resolveFilter`). It resolves column names to leaf indices, checks each leaf against the rule, and converts each literal to the column's physical carrier, so evaluators never repeat a lookup or a type check. A multi-file read resolves against the first file's schema; `FileColumnOrdinals` remaps the leaf indices onto each later file's own leaf order with `ResolvedPredicate.remapColumns`, and a leaf with no counterpart throws.

Tests: `CrossFileColumnOrderTest`.

### The resolved tree

`ResolvedPredicate` is a sealed interface of leaves over physical carriers (`IntPredicate`, `UnsignedLongPredicate`, `Float16Predicate`, `BinaryPredicate`, their set forms, `IsNullPredicate`, `IsNotNullPredicate`, `GeospatialPredicate`), two constants, and `And` / `Or`. It has no negation node. `And` and `Or` refuse an empty child list and flatten nested children of their own kind at construction, so consumers see one level.

The constants are `NoRowPredicate` (no row) and `EveryNonNullRowPredicate` (every non-null row), each the other's negation. `EveryNonNullRowPredicate` matches the rows `IsNotNullPredicate` does but negates differently: a comparison stays unknown on a null row under `not`, so the negation of "every non-null row" is no row, not the null rows. Using `IsNotNullPredicate` for it would make `not(not(p))` return the rows `p` excludes for being null.

Tests: `ResolvedPredicateTest`, `PredicatePathAgreementTest`.

### Leaves

- **Typed literals** convert to the carrier: `LocalDate` to epoch days, `Instant` / `LocalDateTime` to the unit, `BigDecimal` to the unscaled value at the scale, `UUID` and `PqInterval` to their stored bytes, rounding as in [Literals the column cannot hold](#literals-the-column-cannot-hold). A literal past the carrier's range resolves to `NoRowPredicate` on one side and to a comparison with the carrier's extreme on the other.
- **Unsigned `INT`** columns resolve to the `Unsigned*` leaves, decided by the annotation alone, whatever the bit width.
- **`BOOLEAN`** ordered operators resolve to an equality or a constant (`ltEq(true)` is every non-null row, `lt(false)` none), so every evaluator answers a boolean column through one comparison; `ResolvedPredicate.BooleanPredicate` refuses any other operator.
- **Set forms** resolve probe by probe into the carrier's membership test, each probe checked as an equality literal.
- **Null tests on a group** resolve to a leaf below the group and the group's definition level. Any leaf answers whether the group is present; a non-repeated leaf is preferred because it writes one entry per row, which lets a row group be proven to match throughout.
- **Order refusal comes first.** An ordered operator on a type with no order is refused before the literal's type is checked, and the message names the literals the column takes with `eq`, `notEq` and `in`.

Tests: `FilterPredicateResolverTest`, `ResolvedPredicateTest`.

### Negation

`not` is lowered to the leaves by `ResolvedPredicate.negate`: each comparison takes `Operator.invert()` (`gt` becomes `ltEq`), `IsNull` and `IsNotNull` swap, the two constants swap, `And` and `Or` swap by De Morgan, and a set form becomes the conjunction of `notEq` over its probes. A `GeospatialPredicate` has no inverse and throws; the resolver refuses `not` over `intersects` before that is reached.

Tests: `FilterPredicateResolverTest`, `FilterPredicateTest`.

### Binary comparisons

A `BinaryPredicate` and a `BinaryInPredicate` carry a `Comparison`, which fixes both the order the bytes compare in and whether a value has exactly one encoding in the column. The two are one choice because the order is what decides which encodings stand for the same value.

| `Comparison` | Used for | `byteExact` | `BinaryComparator.sliceOrder` |
|---|---|---|---|
| `BYTE_STRING` | byte-ordered columns: text, `BSON`, `UUID`, `INTERVAL`, geometry, unannotated binary | yes | unsigned |
| `FIXED_DECIMAL` | `DECIMAL` over `FIXED_LEN_BYTE_ARRAY`, typed or `byte[]` literal | yes | signed |
| `VARIABLE_DECIMAL` | `DECIMAL` over `BYTE_ARRAY` | no | signed |
| `FIXED_TIMESTAMP` | `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)`, typed or `byte[]` literal | yes | none |
| `INT96_INSTANT` | `Instant` on `INT96` | no | none |
| `STORED_BYTES` | `byte[]` equality on a value-ordered column with several encodings | yes | unsigned |

- **`byteExact`** gates every shortcut that tests bytes rather than order: the Bloom filter probe, the dictionary's byte probe, and the byte-equality batch matchers. Without it, a padded spelling of the literal's value would hash or compare as a miss and its rows would be dropped. A `FLOAT16` `float` literal is not a `BinaryPredicate`; it skips the Bloom filter and is checked against the dictionary by decoded value ([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md)).
- **`sliceOrder`** names the order a batch matcher can compare byte slices in; `NONE` keeps a predicate off the batch path ([RECORD_FILTERING.md](RECORD_FILTERING.md)).

A `byte[]` literal goes through `orderingLiteral`, which names the typed literal of a value-ordered column or returns none for a byte-ordered one. On a byte-ordered column it resolves to `BYTE_STRING`. On a value-ordered one the ordered operators throw, naming the typed literal, and equality resolves as follows:

| Column | `byte[]` equality |
|---|---|
| `DECIMAL` over `FIXED_LEN_BYTE_ARRAY` | `FIXED_DECIMAL` (one encoding per number) |
| `DECIMAL` over `BYTE_ARRAY` | `VARIABLE_DECIMAL` and `STORED_BYTES` |
| `FLOAT16` | `Float16Predicate` of the decoded half and `STORED_BYTES` |
| `INT96` | `STORED_BYTES` |
| `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` | `FIXED_TIMESTAMP` (one encoding per value) |

`STORED_BYTES` reads no bounds, since the column's bounds are written in the order of its values; the value comparison beside it prunes on them, which is sound because a row storing those bytes holds that value. `notEq` is the negation of the pair, so it matches a row holding the same value under another encoding.

Tests: `BinaryDecimalFilterTest`, `tools/predicate-audit`, `FilterPredicateResolverTest`.

### Exhaustive switches

Every per-annotation decision is a `switch` over `LogicalType` (or over `Comparison`) with no `default` arm, so an annotation or comparison added later fails to compile until each decision states its answer:

| Switch | Decides |
|---|---|
| `FilterPredicateResolver.orderingLiteral` | whether a binary column orders as its bytes, and which literal carries its order |
| `ColumnLiterals.logical` | the literals a refusal message names |
| `TextColumns.isText` | whether `getString` reads the column and a `String` is its literal |
| `BoundsReadability.namesAnOrder` | whether the type defines an order, shared by the ordered-operator refusal and bounds readability |
| `BinaryComparator.sliceOrder` | the slice order of a `Comparison` |

Enforced by the compiler.

### Errors

At reader creation a predicate the rule does not admit throws `IllegalArgumentException` naming the column and why it is refused; a literal of a type the column does not take also names the literals it takes, for example `Column 'c' is annotated DATE, which takes LocalDate and int literals, not a BigDecimal`. That covers every refusal above and a name that reaches no node.

## Column addressing

A predicate names its column by a dot-separated path from the schema root (`address.city`). `SchemaPathResolver` walks the schema tree one segment per level and can stop on a group, which is what lets the resolver distinguish "a group" and "a leaf of a `VARIANT` group" from "not found". A leaf is then looked up through `FileSchema.getColumn(String)`. A field whose own name contains a dot cannot be addressed. Untested.

Tests: `PredicatePushDownTest`, `FilterPredicateResolverTest`.

## parquet-java compat translation

`parquet-java-compat` ships shims of parquet-java's `FilterApi`, `Operators`, `FilterPredicate` and `FilterCompat`, and `ParquetReader.Builder.withFilter`. When the reader is built, `FilterConverter` (package-private in `org.apache.parquet.hadoop.util`, reached through `InputFiles.convertFilter`) translates the tree against the opened file's `FileSchema`:

| parquet-java node | Hardwood predicate |
|---|---|
| `Eq`, `NotEq`, `Lt`, `LtEq`, `Gt`, `GtEq` | the comparison record for the value's type, with the column's `ColumnPath.toDotString()` as its path |
| `In` | `in` over the converted values |
| `NotIn` | `not(in(...))` over the converted values |
| `And`, `Or`, `Not` | `and`, `or`, `not` |
| any other `FilterPredicate` implementation | `UnsupportedOperationException`. Untested. |

The shim's `FilterApi` offers the six comparisons, `in`, `notIn` and the three combinators, with parquet-java's signatures; `contains` and `userDefined` are absent, so code using them does not compile against the shim. Hardwood has no set form on `BOOLEAN`, so a boolean `In` becomes `eq` for one value and `or` of both `eq`s for two, which selects the same rows. An empty set is refused where parquet-java refuses it, in the `In` / `NotIn` constructor, with parquet-java's message.

Literal translation:

- `Integer`, `Long`, `Float`, `Double` and `Boolean` become the matching primitive literal and follow the rule as any Hardwood literal does.
- A `Binary` is translated by the column it names, because parquet-java compares it through the column's comparator. On a `DECIMAL` over `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` it becomes a `BigDecimal` at the column's scale (an empty `Binary` is zero). On a `FLOAT16` a 2-byte `Binary` becomes the `float` it encodes. Every other `Binary`, including one of another width on a `FLOAT16`, becomes a `byte[]` and follows the byte rule, so an ordered `Binary` on an `INT96` or a `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` is refused.
- A `null` value, as a comparison literal or as a set element, throws `IllegalArgumentException` when the reader is built. The shim has no null literal and no null test; `isNull` / `isNotNull` exist on Hardwood's own reader API.

The translated tree evaluates under Hardwood's semantics, three-valued logic included, so `notEq` and `notIn` through the shim return no null rows (parquet-java's own record filter returns them).

Tests: `ParquetReaderCompatTest` (parquet-java-compat).

## Differences from parquet-java and DuckDB

On conforming files from mainstream writers, with the typed literal of each column, Hardwood, parquet-java and DuckDB (the versions `tools/predicate-audit` pins) return the same rows. The differences below are measured by the audit on fixtures written by parquet-java and PyArrow and read by all three engines. Each row falls into one of three groups: Hardwood follows its rule where another engine chose differently (a migrated query can silently return different rows); another engine has a defect; or only unusual or non-conforming files show the difference. The row numbers are stable: the comments in `tools/predicate-audit/baseline.tsv` cite them as "Design row N".

| # | Data | Predicate | Hardwood | parquet-java | DuckDB | Cause | Relevance |
|---|---|---|---|---|---|---|---|
| 1 | `INT32 i`: `20, 22, null` | `notEq(i, 20)` | `22` | `22, null` | `22` | Rule: three-valued logic | **High** for code migrated from parquet-java: nulls under `notEq` / `notIn` |
| 2 | `FLOAT f` and `DOUBLE d` with `IEEE_754_TOTAL_ORDER` (what parquet-java writes by default since 1.18.0): `NaN` as `7fc00000`, `ffc00000` and `7f800001`; `FLOAT16 f16`: `NaN` as `00 7E` and `00 FE` | `eq(f, Float.NaN)`, `gtEq(d, Double.NaN)`; `gt(f, 10.0f)`; `gt(f16, 1.0f)` | all three; all three; both | `7fc00000` only; `7fc00000` and `7f800001`; `00 7E` only | all three; all three; both | Rule: `Float.compare`, where every `NaN` is one value above every number; parquet-java compares a column that declares the total order by it, which tells `NaN` payloads apart and sorts a negative `NaN` below `-Infinity` | Low; needs a `NaN` other than the canonical `7fc00000` |
| 3 | `DOUBLE d`, PyArrow: row group `1.0, 2.0, NaN, 3.0` with bounds `[1.0, 3.0]` | `gt(d, 5.0)`, `eq(d, NaN)` | includes `NaN` | includes `NaN` | row group dropped | DuckDB defect: pruning ignores `NaN` outside the bounds | Medium on float data with `NaN` (Arrow writers) |
| 4 | `DECIMAL(9, 2)` over `INT32`: `1.25` | `eq(dec, 125)` | `1.25` | `1.25` | none (`125.00`) | Rule: `int` is the unscaled value | Medium; `BigDecimal` is the natural literal |
| 5 | same | `eq(dec, 1.255)` | throws | no spelling | none | Rule 4: equality literal the column cannot hold | Low; fails loudly |
| 6 | `TIMESTAMP(NANOS)`, PyArrow: `t`, `t+1ns` … `t+7ns` | `gt(ts, t+4ns)` | `t+5ns` … `t+7ns` | same | none (`eq` returns all) | DuckDB defect: reads nanoseconds as microseconds | Low; nanosecond-exact filters only |
| 7 | `FLOAT f`, `DOUBLE d`: `-0.0, +0.0` | `eq(d, 0.0)`; `lt(d, 0.0)` | `+0.0`; `-0.0` | `+0.0`; `-0.0` | both; none | Rule: `Double.compare`, as parquet-java | Low |
| 8 | `UINT_32 u`: `0, 4000000000` | `lt(u, -1)` | both (`-1` is `0xFFFFFFFF`) | both | none | Rule: bit-pattern literal, as parquet-java | Low; unsigned columns are rare on the JVM |
| 9 | `INTERVAL iv`: 310 months, 0 days, 310 s | `eq(iv, new PqInterval(0, 9300, 310_000))` | none | no spelling | the row (normalised) | Rule: `INTERVAL` defines no conversion between components | Low |
| 10 | `INT96 ts` | `gt(ts, …)` | by instant | by bytes as a signed big-endian integer | by instant | parquet-java's comparator; the shim refuses an ordered `Binary` | Low |
| 11 | `DECIMAL(30, 3)` over `BYTE_ARRAY` storing `0.002` as `00 02`, Bloom filter | `Binary` / `byte[]` `02` | none | the row, except where the Bloom filter drops it | no spelling | parquet-java defect: Bloom and dictionary test bytes, rows test value | Low; needs a padded encoding |
| 12 | `INT32` annotated `INT(8)` storing `1000` | `eq(i8, 1000)` | the row | the row | none (reads `-24`) | Non-conforming file; Hardwood follows `getInt` | Low |
| 13 | any column, through `parquet-java-compat` | `FilterApi.eq(intColumn("x"), null)` | throws | the null rows | — | The shim supports no null literal; use Hardwood's `isNull` | Medium for shim users; fails loudly |
| 14 | `DECIMAL(9, 2)` over `FIXED_LEN_BYTE_ARRAY(9)` storing `0.20` | `Binary` / `byte[]` `14` | throws | the row, except where the dictionary or Bloom filter drops it | no spelling | Rule 4, and the parquet-java defect of row 11 | Low; needs a literal of another width |
| 15 | `DECIMAL(30, 3)` over `BYTE_ARRAY` storing `0` as no bytes, Bloom filter | `eq(dec, BigDecimal.ZERO)` | the row | none | the row | parquet-java defect: the Bloom filter tests the literal's bytes `00` | Low; needs an empty encoding |
| 16 | `INTERVAL` written by parquet-java (`converted_type = INTERVAL`, `logicalType = UNKNOWN`) | `eq(iv, PqInterval)` | the row | the row | none (read as `INTEGER`) | DuckDB defect: reads the column as `INTEGER` | Low; DuckDB only |

## Validation

Two oracles of the rule check every read path, each independent of `FilterPredicateResolver`:

| | `PredicatePathAgreementTest` | `tools/predicate-audit` |
|---|---|---|
| Runs | every unit-test build | PR build, on changes to the paths its README lists |
| Stored values from | the reader, held first to `<corpus>.values.tsv.gz` from `tools/simple-datagen.py` | the fixture generator (`Columns.java`), never Hardwood's accessors |
| Fixtures | PyArrow corpora: single, multi, dictionary, Bloom, page index, nested, `INT96`, 12-byte timestamps | parquet-java writes, plus footer rewrites (converted-type-only, dropped annotations, annotated 12-byte timestamps) |
| Also checks | | resolver matrix, Bloom and dictionary consultation, accessor round-trip, parquet-java and DuckDB comparison |

Both answer each case through the `RowReader` (default, forced onto its record-level path, and without metadata filtering) and the `ColumnReader` (with and without metadata filtering), so bounds, page index, dictionary and Bloom filter each decide a predicate that the record-level comparison decides again. The audit's `run.sh` fails when its findings differ from `tools/predicate-audit/baseline.tsv` in either direction. The baseline holds accepted engine differences, logical reads that throw on a stored value the type cannot represent, and engine read failures, each with a comment naming its cause; a disagreement between Hardwood and the rule is fixed, not baselined. `tools/predicate-audit/README.md` describes running and extending it. Neither checks `intersects` row by row, since it decides row groups: the audit reports its answers without judging them, and `GeospatialEndToEndTest` covers it.

## Boundaries

- Filtering on the sub-paths of a shredded `VARIANT` is not supported; its leaves take `isNull` / `isNotNull` only ([#309](https://github.com/hardwood-hq/hardwood/issues/309)).
- `intersects` answers per row group; row-level spatial filtering is [#414](https://github.com/hardwood-hq/hardwood/issues/414).
- A leaf below a repeated path takes no predicate, so there is no "any element" or "every element" test on a `LIST` or `MAP`.
- The compat shim has no `contains`, `userDefined` or null test, and no null literal.

Changing any of the following needs a design discussion: the literal rule and its per-type table, three-valued logic, the `Float.compare` order, the refusal of unholdable equality literals, the independence of results from `ColumnOrder`, making the predicate records a supported construction route, and giving `ResolvedPredicate` a negation node.
