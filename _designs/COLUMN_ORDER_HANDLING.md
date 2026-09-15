# Plan: Surface `ColumnOrder` and read float/double bounds under it (#595)

**Status: Implemented**

## Context

The Parquet footer carries an optional `FileMetaData.column_orders` field (Thrift field 7): a
`list<ColumnOrder>` with one entry per leaf column, in schema order. `ColumnOrder` is a union whose
members name the order a column's `min`/`max` statistics are written in:

| Union field | Member | Meaning |
|---|---|---|
| 1 | `TypeDefinedOrder` (`TYPE_ORDER`) | Order defined by the physical/logical type. For `FLOAT`/`DOUBLE` this is *signed comparison of the represented value* with documented NaN / ±0 compatibility rules. |
| 2 | `IEEE754TotalOrder` (`IEEE_754_TOTAL_ORDER`) | The IEEE 754 total order, for `FLOAT`, `DOUBLE` and `FLOAT16` only. |

PyArrow writes `TYPE_ORDER` for every column. parquet-java writes `IEEE_754_TOTAL_ORDER` for
`FLOAT`, `DOUBLE` and `FLOAT16` columns since 1.18.0, and `TYPE_ORDER` for the others. A dataset
built by both writers mixes the two orders across its files.

The column order describes the statistics only. Predicates compare values by `Float.compare` /
`Double.compare` whatever order a file declares (see `PREDICATE_LITERALS.md`), so the order decides
how a unit's bounds are read, never which rows match.

### Where the orders differ

`Float.compare` orders every number as the IEEE 754 total order does, `-0.0` below `+0.0` included,
and treats every `NaN` as one value above `+Infinity`. `TYPE_ORDER` and the total order therefore
differ for pruning only at **NaN** and **±0.0**:

- **NaN**: under both orders a writer keeps `NaN` out of the bounds and records `nan_count`; only a
  unit whose every non-null value is `NaN` carries `NaN` bounds under the total order. `MinMaxStats`
  discards a pair holding `NaN` where it sources the bounds, so no comparator is handed one, and
  `nan_count` decides whether the bounds rule out a `NaN` row.
- **±0.0**: under `TYPE_ORDER` the spec leaves the zeroes interchangeable. A writer should record a
  zero minimum as `-0.0` and a zero maximum as `+0.0` (PyArrow 24.0.0 does), and a reader should
  assume a `+0` minimum may hide `-0` and a `-0` maximum may hide `+0`. Under the total order the
  zero bounds are exact.

## Design

### Surface the field

- Public enum `dev.hardwood.metadata.ColumnOrder` with `TYPE_DEFINED_ORDER`,
  `IEEE754_TOTAL_ORDER`, and `UNKNOWN`.
- `FileMetaData` has a `List<ColumnOrder> columnOrders` component. When `column_orders` is absent
  the list is empty (the implicit, type-defined ordering applies to all columns).
- `ColumnOrderReader` decodes one `ColumnOrder` union from the Thrift Compact stream;
  `FileMetaDataReader` decodes field 7 into the list.

A union member Hardwood does not recognise (any field id other than 1 or 2) decodes to
`ColumnOrder.UNKNOWN`, following the reader's convention for non-fatal unknown union/enum members:
`LogicalType` skips and yields `null`, `Encoding` maps to `Encoding.UNKNOWN`. `BoundsReadability`
reads each file's own `column_orders` and marks the bounds of a column under an unrecognised order
unreadable, so they prune nothing.

### ±0 widening

`StatisticsFilterSupport.canDropFloat` / `canDropDouble` and the `IN`-list variants widen a zero
bound to its total-order extreme (`min` ⇒ `-0.0`, `max` ⇒ `+0.0`) before the `Float.compare` checks,
and `MinMaxStats` applies the same widening before it tests a pair for inversion, so `(+0, -0)` is
not read as inverted.

The widening applies under every declared order. A predicate is resolved once per read, against the
first file's schema, and applies to the row groups of every file; the files of one read may declare
different orders, so a flag taken from one file would skip the widening on a sibling that needs it.
Widening only enlarges the candidate range, so on a total-order column it costs a unit whose zero
bound excludes the opposite zero, which the record-level filter then drops, and never a matching row.

## Testing

- `ColumnOrderReaderTest` decodes the `TYPE_ORDER`, `IEEE_754_TOTAL_ORDER`, unrecognised, and empty
  union forms from raw Thrift bytes.
- `ColumnOrdersTest` asserts `column_orders` is surfaced as `TYPE_DEFINED_ORDER` for a PyArrow
  fixture; `TotalOrderFloatReadTest` (`parquet-testing-runner`) asserts `IEEE754_TOTAL_ORDER` and
  `nan_count` are surfaced for a file written by parquet-java, and that zero and `NaN` predicates
  return the rows they match.
- `NaNStatisticsFilterTest` asserts a `±0` bound is not dropped by a `== ∓0` predicate and that
  non-zero predicates still prune against zero bounds.
- `MixedColumnOrderPruningTest` reads a total-order file followed by a type-defined file whose
  `+0` minimum hides a `-0`, and asserts `eq(f, -0.0f)` returns that row.
