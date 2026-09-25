<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# parquet-java Compat Filters

Filter predicates of the `hardwood-parquet-java-compat` module. For setting up a filtered read, see [Parquet-Java Compatibility](../how-to/compat.md#filter-pushdown).

## Predicates

`FilterApi` offers these predicates:

| Kind | Methods | Columns |
|------|---------|---------|
| Comparison | `eq`, `notEq` | all six column types |
| Comparison | `lt`, `ltEq`, `gt`, `gtEq` | all but `booleanColumn` |
| Set | `in`, `notIn` | all six column types |
| Logical | `and`, `or`, `not` | any predicates |

`contains` and `userDefined` are not available. `in` and `notIn` take a non-empty `Set`; an empty one throws `IllegalArgumentException` when the predicate is built.

## `Binary` literals

A `Binary` literal, in a comparison or in a set, converts by the type of the column it filters:

| Column | Literal |
|--------|---------|
| `DECIMAL` over `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` | the big-endian two's-complement unscaled value at the column's scale, of any width; empty bytes are zero |
| `FLOAT16`, with a 2-byte literal | the IEEE half the two little-endian bytes encode |
| any other | the stored bytes |

## Nulls

A `null` literal throws `IllegalArgumentException` when the reader is built, whether it is the value of a comparison or an element of an `in` / `notIn` set. The shim has no null test; Hardwood's own reader API filters on nulls with `isNull` / `isNotNull` (see [Query Controls](../how-to/query-controls.md)).

A null row matches no comparison and no set predicate, so `notEq` and `notIn` return no null rows, where parquet-java's record filter returns them. [Compatibility Philosophy](../concepts/compatibility-philosophy.md) explains this choice.
