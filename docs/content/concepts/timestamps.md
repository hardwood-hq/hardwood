<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Timestamp Semantics

The Parquet TIMESTAMP logical type carries an `isAdjustedToUTC` flag that picks between two
genuinely different kinds of value. Hardwood splits its accessor surface along the same line — a
`getTimestamp` that returns `Instant` and a `getLocalTimestamp` that returns `LocalDateTime` —
rather than papering over the difference with a single accessor.

## Two kinds of timestamp

- **`isAdjustedToUTC = true`** is an absolute point on the global timeline. The stored value counts
  time units since the Unix epoch in UTC, and it identifies the same moment everywhere. Java's
  `Instant` models exactly this: a count from the epoch with no attached zone, comparable across
  systems.
- **`isAdjustedToUTC = false`** is a wall-clock reading — a calendar date and time-of-day with no
  zone information. "2026-06-04 09:00" means whatever local clock recorded it; it does *not* pin a
  moment until a zone is supplied externally. Java's `LocalDateTime` models exactly this.

The two are not interconvertible without a time zone, and silently treating one as the other shifts
values by the local UTC offset — the classic source of off-by-hours bugs. A single accessor
returning one Java type would force that lossy coercion on half of all timestamp columns, so the
API keeps them distinct.

## The split accessor pair

Because the column's flag already records which kind it is, each accessor enforces the matching
flag rather than guessing: `getTimestamp` is the `Instant` accessor and `getLocalTimestamp` the
`LocalDateTime` one, and calling the wrong one for a column is a programming error that throws — the
column's semantics, not the caller's expectation, win. The exact runtime contract (which flag each
requires, the exception thrown) lives in [Typed Accessors](../reference/accessors.md). When a
column's kind isn't known statically, branching on the flag — or the generic `getValue` accessor,
which returns `Instant` or `LocalDateTime` per the flag — recovers the right type without a guess.

## TIME's informational flag

The TIME logical type carries the same `isAdjustedToUTC` flag, but the situation is different:
`LocalTime` has no zone of its own, so there is no second Java type to split toward. `getTime`
returns `LocalTime` either way and the flag is purely informational.

## Legacy INT96

Timestamps written by older Spark and Hive in the deprecated INT96 physical type carry no
`isAdjustedToUTC` field at all. By the Spark / Hive convention they denote instants, so Hardwood
reads them as `Instant` through `getTimestamp`.
