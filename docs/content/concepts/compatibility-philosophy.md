<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Compatibility Philosophy

Hardwood aims to read every file that Apache parquet-java reads, while in a few specific places
applying *stricter* semantics than parquet-java does. That combination is deliberate, and the
principle behind it makes the individual divergences read as a consistent stance rather than a
list of quirks.

For the drop-in API and its exact behavior, see
[parquet-java Compatibility](../how-to/compat.md); for the filter semantics, see
[Filter, Project, Limit, and Split](../how-to/query-controls.md#null-handling).

## The principle: liberal on input, strict on semantics

Two goals are in tension, and Hardwood resolves them on different axes:

- **Read what's out there.** Parquet files in the wild were written by many tools across many
  years — parquet-mr, Arrow, Spark, Hive, PyArrow — using legacy encodings, deprecated types,
  and annotations that newer specs dropped. Hardwood reads these transparently: legacy 2-level
  list encodings, INT96 timestamps, the legacy `converted_type` annotations that predate the
  modern logical-type union, `NULL`-typed columns, FLOAT16. On the *input* side, Hardwood is
  liberal — if parquet-java can read it, Hardwood aims to.
- **Don't silently produce wrong results.** On the *semantic* side, where a behavior is a matter
  of correctness rather than file compatibility, Hardwood chooses the well-defined answer even
  when that diverges from parquet-java's historical behavior. This follows the project's
  fail-early, no-silent-wrong-results stance: a surprising-but-correct result beats a
  familiar-but-wrong one.

Liberal on what bytes it accepts; strict on what those bytes are defined to mean.

## Where strictness surfaces

### SQL three-valued logic for comparison predicates

The clearest case is `notEq` and its siblings. Hardwood applies uniform SQL three-valued logic
to every comparison predicate: any comparison against a null column value yields UNKNOWN, and
UNKNOWN rows are not returned. So `notEq("x", v)` does **not** return rows where `x` is null —
just as `eq`, `lt`, and the rest don't.

parquet-java treats `null <> v` as true and so includes null rows in `notEq`, which breaks the
SQL identity `not(gt(x, v)) ≡ ltEq(x, v)` on null rows. Hardwood preserves that identity across
all operators, including under `not(...)`. The cost is that a user porting a `notEq` filter from
parquet-java may see fewer rows; the benefit is that filter algebra behaves predictably and
matches what a SQL engine would do.

The looser behavior is one explicit composition away when you want it:

```java
// parquet-java's notEq semantics: "not equal to v, OR null"
FilterPredicate.or(FilterPredicate.notEq("x", v), FilterPredicate.isNull("x"));
```

Making null-inclusion explicit, rather than hiding it inside one operator's special case, is the
point — the reader can see in the code which rows the filter admits.

### Schema validation across multiple files

When a reader spans multiple files, the first file's schema is the reference, and every
subsequent file is validated against it *as it is opened*: each projected column must exist with
a matching physical type, logical type, and repetition type, or a `SchemaIncompatibleException`
is thrown up front. A silently mismatched column that produced garbage values mid-stream would be
the worse outcome. Non-projected columns aren't checked, so files may carry extra columns — again
liberal on the parts that don't affect correctness, strict on the parts that do. See
[Read Multiple Files as One Dataset](../how-to/multi-file.md).

## The drop-in compat module

For callers who want parquet-java's *API* without rewriting code, the
`hardwood-parquet-java-compat` module implements the `org.apache.parquet.*` interfaces backed by
Hardwood. It is an API-compatibility shim, not a behavioral clone: filters routed through it
still evaluate under Hardwood's semantics. The module is mutually exclusive with parquet-java on
the classpath — see [parquet-java Compatibility](../how-to/compat.md).

## Further reading

- [parquet-java Compatibility](../how-to/compat.md) — the drop-in API surface and its constraints.
- [Filter, Project, Limit, and Split](../how-to/query-controls.md#null-handling) — the full null
  semantics and the supported predicate surface.
