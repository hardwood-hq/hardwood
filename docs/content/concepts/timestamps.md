<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Timestamp Semantics

The Parquet TIMESTAMP logical type carries an `isAdjustedToUTC` flag that picks between two
different kinds of value. Hardwood splits its accessor surface along the same line:
`getTimestamp` returns an `Instant` and `getLocalTimestamp` returns a `LocalDateTime`.

## Two kinds of timestamp

- **`isAdjustedToUTC = true`** is an absolute point on the global timeline. The stored value counts
  time units since the Unix epoch in UTC, and it identifies the same moment everywhere. Java's
  `Instant` models this: a count from the epoch with no attached zone, comparable across
  systems.
- **`isAdjustedToUTC = false`** is a wall-clock reading — a calendar date and time-of-day with no
  zone information. "2026-06-04 09:00" means whatever local clock recorded it; it does *not* pin a
  moment until a zone is supplied externally. Java's `LocalDateTime` models it.

The two are not interconvertible without a time zone, and silently treating one as the other shifts
values by the local UTC offset — the classic source of off-by-hours bugs. A single accessor
returning one Java type would force that lossy coercion on half of all timestamp columns, so the
API keeps them distinct.

## The split accessor pair

Because the column's flag already records which kind it is, each accessor enforces the matching
flag, and calling the wrong one for a column is a programming error that throws. The exact
runtime contract, including how to read a column whose kind isn't known statically, lives in
[Typed Accessors](../reference/accessors.md).

Filter predicates follow the same split. A `FilterPredicate` literal on a UTC-adjusted column is an
`Instant` and on a local one a `LocalDateTime`, so a predicate compares the same kind of value the
column's accessor returns; the other kind is rejected when the reader is built, for the same reason
the other accessor throws (see [Predicate literals by column type](../reference/query-controls.md#predicate-literals-by-column-type)).

The TIME logical type carries the same flag, but `LocalTime` has no zone of its own, so there is no
second Java type to split toward (see [Type mismatches](../reference/accessors.md#type-mismatches)).

## Two physical carriers

The TIMESTAMP annotation is stored in two physical types, which differ in the range they span.
Both count the same three units, milliseconds, microseconds and nanoseconds,
since the epoch. An `INT64` spans about 292 million years either side of 1970 in milliseconds and
292,000 years in microseconds, but only about 585 years in nanoseconds, from 1677 to 2262. A
`FIXED_LEN_BYTE_ARRAY(12)` holds the count in 96 bits, which spans every `Instant` in any unit.

So the 12-byte carrier matters for a column that needs nanosecond precision beyond 1677 to 2262,
such as a SQL `TIMESTAMP(9)` covering the years 0001 to 9999. A millisecond or microsecond column
reaches those years on an `INT64` already.

The annotation means the same on both, so an application reads and writes either carrier alike
(see [Timestamps over FIXED_LEN_BYTE_ARRAY(12)](../reference/accessors.md#timestamps-over-fixed_len_byte_array12)).

## Legacy INT96

Timestamps written by older Spark and Hive in the deprecated INT96 physical type carry no
`isAdjustedToUTC` field at all. By the Spark / Hive convention they denote instants, so Hardwood
reads them as `Instant` (see [Legacy INT96 timestamps](../reference/accessors.md#legacy-int96-timestamps)).
