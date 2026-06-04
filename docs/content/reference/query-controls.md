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
| Set operators | `in` (int, long), `inStrings` |
| Null operators | `isNull`, `isNotNull` (any type) |
| Physical types (comparison) | `int`, `long`, `float`, `double`, `boolean`, `String` |
| Logical types (comparison) | `LocalDate`, `Instant`, `LocalTime`, `BigDecimal`, `UUID` |
| Combinators | `and`, `or`, `not` (`and` / `or` accept varargs for three or more conditions) |

All predicates, including those wrapped in `not`, are pushed down to the statistics level for
row-group and page skipping.

The logical-type factories validate the column's logical type at reader creation: `BigDecimal`
predicates require a `DECIMAL` column and `UUID` predicates require a `UUID` column. Applying them
to a plain `FIXED_LEN_BYTE_ARRAY` column without the corresponding logical-type annotation throws
`IllegalArgumentException`. Raw physical-type predicates (`int`, `long`, etc.) remain available for
columns without logical types or for filtering on the underlying physical value directly. Filters
work with all reader types — `RowReader`, `ColumnReader`, `AvroRowReader`, and across multi-file
readers.

## Column projection forms

| Form | Description |
|------|-------------|
| `ColumnProjection.all()` | Read all columns (default) |
| `ColumnProjection.columns("id", "name")` | Read specific columns by name |
| `ColumnProjection.columns("address")` | Select an entire struct and all its children |
| `ColumnProjection.columns("address.city")` | Select a specific nested field (dot notation) |
