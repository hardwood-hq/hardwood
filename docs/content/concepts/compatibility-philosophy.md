<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Compatibility Philosophy

Hardwood aims to read every file that Apache parquet-java reads, while in a few specific places
applying *stricter* semantics than parquet-java does. Each of those divergences follows from the
principle described below.

## The principle: liberal on input, strict on semantics

Two goals are in tension, and Hardwood resolves them on different axes:

- **Read what's out there.** Parquet files in the wild were written by many tools across many
  years (parquet-mr, Arrow, Spark, Hive, PyArrow) using legacy encodings, deprecated types,
  and annotations that newer specs dropped. Hardwood reads these transparently: most legacy
  list-encoding variants, INT96 timestamps, the legacy `converted_type` annotations that
  predate the modern logical-type union, `NULL`-typed columns, FLOAT16. The tolerance also
  extends *forward*: a logical-type annotation newer than Hardwood recognizes is ignored rather
  than rejected, and the column's physical type is exposed, so a file written by a tool that
  has adopted a future logical type still reads.
- **Don't silently produce wrong results.** On the *semantic* side, where a behavior is a matter
  of correctness rather than file compatibility, Hardwood chooses the well-defined answer even
  when that diverges from parquet-java's historical behavior.

## Where strictness surfaces

### SQL three-valued logic for comparison predicates

Hardwood applies SQL three-valued logic to every comparison predicate, so `notEq("x", v)` returns
no rows where `x` is null (see [Null Handling](../how-to/query-controls.md#null-handling)).
parquet-java treats `null <> v` as true and so includes null rows in `notEq`, which breaks the
SQL identity `not(gt(x, v)) ≡ ltEq(x, v)` on null rows. Hardwood preserves that identity across
all operators, including under `not(...)`. The cost is that a user porting a `notEq` filter from
parquet-java may see fewer rows; the benefit is that filter algebra behaves predictably and
matches what a SQL engine would do.

### Schema validation across multiple files

A reader spanning multiple files checks every column a read touches against the first file's
schema, and throws `SchemaIncompatibleException` on a mismatch (see
[Reading Multiple Files](../how-to/multi-file.md)). The match is by field path, never by
position. A Parquet footer lists column chunks in the order of the schema's flattened leaves, so a
column's ordinal belongs to the file that was written, not to the column; two files that declare
the same columns in a different order describe the same data.

## The drop-in compat module

The `hardwood-parquet-java-compat` module is an API-compatibility shim, not a behavioral clone:
filters routed through it evaluate under Hardwood's semantics. See
[parquet-java Compatibility](../how-to/compat.md).

## Further reading

- [parquet-java Compatibility](../how-to/compat.md) — the drop-in API surface and its constraints.
- [parquet-java Compat Filters](../reference/parquet-java-compat.md) — the filter predicates the
  compat module accepts.
- [Filter, Project, Limit, and Split](../how-to/query-controls.md#null-handling) — the full null
  semantics and the supported predicate surface.
