<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Typed Accessors

`RowReader` — and the nested `PqStruct` / `PqList` / `PqMap` flyweights — decode each column to its
logical-type Java representation through typed accessor methods. This page is the full
correspondence between accessor, Parquet type, and Java type, together with the null- and
type-mismatch contracts every accessor obeys. For the task-oriented walkthrough, see
[Read Row by Row](../how-to/row-reader.md).

## Accessor type mapping

All accessors are available in two forms — name-based (`getInt("column_name")`) and index-based
(`getInt(columnIndex)`); see [Index-based access](../how-to/row-reader.md#index-based-access).

| Method | Physical Type | Logical Type | Java Type |
|--------|--------------|-------------|-----------|
| `getBoolean` | BOOLEAN | | `boolean` |
| `getInt` | INT32 | | `int` |
| `getLong` | INT64 | | `long` |
| `getFloat` | FLOAT, or FIXED_LEN_BYTE_ARRAY(2) | FLOAT16 (optional) | `float` |
| `getDouble` | DOUBLE | | `double` |
| `getBinary` | BYTE_ARRAY | BSON (optional) | `byte[]` |
| `getString` | BYTE_ARRAY | STRING or JSON | `String` |
| `getDate` | INT32 | DATE | `LocalDate` |
| `getTime` | INT32 or INT64 | TIME | `LocalTime` |
| `getTimestamp` | INT64, or legacy INT96 | TIMESTAMP (`isAdjustedToUTC = true`) | `Instant` |
| `getLocalTimestamp` | INT64 | TIMESTAMP (`isAdjustedToUTC = false`) | `LocalDateTime` |
| `getDecimal` | INT32, INT64, or FIXED_LEN_BYTE_ARRAY | DECIMAL | `BigDecimal` |
| `getUuid` | FIXED_LEN_BYTE_ARRAY | UUID | `UUID` |
| `getInterval` | FIXED_LEN_BYTE_ARRAY(12) | INTERVAL | `PqInterval` |
| `getStruct` | | | `PqStruct` |
| `getList` | | LIST | `PqList` |
| `getMap` | | MAP | `PqMap` |
| `getVariant` | BYTE_ARRAY pair | VARIANT | `PqVariant` |
| `isNull` | Any | Any | `boolean` |

All methods are available as both `method(name)` and `method(index)`.

## Null handling

Primitive accessors (`getInt`, `getLong`, `getFloat`, `getDouble`, `getBoolean`) throw
`NullPointerException` if the field is null — always check `isNull()` first. Object accessors
(`getString`, `getDate`, `getTimestamp`, `getLocalTimestamp`, `getDecimal`, `getUuid`,
`getInterval`, `getStruct`, `getList`, `getMap`) return `null` for null fields.

## Type mismatches

Requesting the wrong type for a column (e.g. `getInt` on a `LONG` column, `getDate` on a `STRING`
column) is a programming error; the call fails at runtime with an unchecked exception. The
specific exception type is unspecified and may change between releases — do not catch it as part of
normal control flow. If the column type isn't known statically, check it up front via
`reader.getFileSchema().getColumn(name)` and inspect the returned `ColumnSchema`'s `type()` /
`logicalType()` — see [Inspect File Metadata](../how-to/metadata.md).

The `getTimestamp` / `getLocalTimestamp` pair is split along the column's `isAdjustedToUTC` flag:
`getTimestamp` requires `isAdjustedToUTC = true` and returns `Instant`; `getLocalTimestamp`
requires `isAdjustedToUTC = false` and returns `LocalDateTime`. Calling the wrong one for a column
throws `IllegalStateException` naming the column and the actual flag value. If the kind isn't known
statically, branch on `((LogicalType.TimestampType) column.logicalType()).isAdjustedToUTC()` before
the accessor call, or use the generic `getValue` accessor, which returns `Instant` or
`LocalDateTime` per the column's flag. For why the pair is split, see
[Timestamp Semantics](../concepts/timestamps.md).

The TIME logical type also carries an `isAdjustedToUTC` flag, but `LocalTime` has no zone of its
own, so `getTime` returns `LocalTime` either way and the flag is informational — inspect
`((LogicalType.TimeType) column.logicalType()).isAdjustedToUTC()` if the distinction matters to
your application.

## FLOAT16 columns

`getFloat` accepts FLOAT16 columns (`FIXED_LEN_BYTE_ARRAY(2)` annotated with the `FLOAT16` logical
type) and decodes the 2-byte IEEE 754 half-precision payload to a single-precision `float`. The
widening is lossless — half-precision NaN, ±Infinity, and signed zero round-trip cleanly, and the
original NaN bit pattern is preserved (the Parquet spec does not canonicalize NaNs on write). Use
`Float.isNaN(value)` for NaN checks rather than equality. As with all primitive accessors,
`isNull()` must be checked before `getFloat()` since FLOAT16 columns can be optional.

## Legacy INT96 timestamps

Parquet files written by older versions of Apache Spark and Hive store timestamps in the deprecated
INT96 physical type without a TIMESTAMP logical type annotation. `getTimestamp` detects INT96
automatically and decodes it to an `Instant`; no caller-side handling is required.

## Legacy converted-type annotations

Writers predating the modern logical-type union (older parquet-mr / Hive / Impala / Spark) annotate
primitive columns with only a legacy `converted_type` and no `logicalType`. Hardwood promotes each
one to its logical type, so the column decodes through the normal typed accessor with no
caller-side opt-in:

| `converted_type` | Accessor | Java type |
|------------------|----------|-----------|
| `UTF8` | `getString` | `String` |
| `JSON` | `getString` | `String` |
| `ENUM`, `BSON` | `getBinary` | `byte[]` |
| `DATE` | `getDate` | `LocalDate` |
| `DECIMAL` | `getDecimal` | `BigDecimal` |
| `TIME_MILLIS`, `TIME_MICROS` | `getTime` | `LocalTime` |
| `TIMESTAMP_MILLIS`, `TIMESTAMP_MICROS` | `getTimestamp` | `Instant` |
| `INT_8`, `INT_16`, `INT_32`, `INT_64` | `getValue` | `Byte` / `Short` / `Integer` / `Long` |
| `UINT_8`, `UINT_16`, `UINT_32`, `UINT_64` | `getValue` | `Integer` / `Long` (raw two's-complement bit pattern) |
| `INTERVAL` | `getInterval` | `PqInterval` |

`TIME_*` columns decode to a UTC-normalized `LocalTime` time-of-day and `TIMESTAMP_*` columns to a
UTC-normalized `Instant`, matching the parquet-format backward-compatibility rule for these
annotations. Unsigned columns preserve the stored bit pattern — reinterpret with
`Integer.toUnsignedLong` / `Long.toUnsignedString` for the unsigned magnitude. When a file carries
both a `converted_type` and a modern `logicalType`, the `logicalType` takes precedence.

The MAP group annotation has a legacy form too: some older parquet-mr / Hive / Impala files annotate
only the inner repeated `key_value` group with `MAP_KEY_VALUE` and leave the outer group unannotated.
Hardwood recognizes this form as a map, so `getMap` returns a `PqMap` and `getColumn(...).logicalType()`
reports a `MapType` exactly as it does for the modern `MAP` annotation.
