# Plan: Surface `ColumnOrder` and apply it to float/double pruning (#595)

**Status: Implemented**

## Context

The Parquet footer carries an optional `FileMetaData.column_orders` field (Thrift field 7): a
`list<ColumnOrder>` with one entry per leaf column, in schema order. `ColumnOrder` is a union with
two members:

| Union field | Member | Meaning |
|---|---|---|
| 1 | `TypeDefinedOrder` (`TYPE_ORDER`) | Order defined by the physical/logical type. For `FLOAT`/`DOUBLE` this is *signed comparison of the represented value* with documented NaN / ±0 compatibility rules. |
| 2 | `IEEE754TotalOrder` (`IEEE_754_TOTAL_ORDER`) | The IEEE 754 total order. |

Hardwood prunes `FLOAT`/`DOUBLE` row groups and pages with `Float.compare` / `Double.compare`, which
implement the IEEE 754 *total order*.

### What is and isn't a problem

For `FLOAT`/`DOUBLE`, `TYPE_ORDER` and total order diverge only at **NaN** and **±0.0**. Everywhere
else they are identical, so the choice of order is irrelevant for pruning every finite, non-zero value.

- **NaN** bounds are already neutralised: `StatisticsFilterSupport.canDropFloat` / `canDropDouble`
  return `false` whenever a bound is NaN and never prune.
- **±0.0**: a spec-compliant `TYPE_ORDER` writer normalises zero bounds to `-0.0` for min and `+0.0`
  for max. PyArrow 24.0.0 does exactly this: a float column whose minimum is zero is written with min
  `-0.0` (raw `0x00000080`) and a column whose maximum is zero with max `+0.0` (raw `0x00000000`).
  With these normalised bounds, total-order pruning is already correct. For writers that do not
  normalise, the spec instead gives readers a compatibility rule — a `+0` min may also cover `-0`, a
  `-0` max may also cover `+0` — which the [order-aware widening](#order-aware-0-pruning) below applies.

PyArrow and parquet-mr emit `TYPE_ORDER` for every column, floats included. For these the widening is
a no-op (their bounds are already normalised); it exists to honour the spec's reader rule for the
type-defined order, keeping non-normalising and older files correct.

## Design

### Surface the field

- New public enum `dev.hardwood.metadata.ColumnOrder` with `TYPE_DEFINED_ORDER`,
  `IEEE754_TOTAL_ORDER`, and `UNKNOWN`.
- `FileMetaData` gains a `List<ColumnOrder> columnOrders` component. When `column_orders` is absent
  the list is empty (the implicit, type-defined ordering applies to all columns).
- `ColumnOrderReader` decodes one `ColumnOrder` union from the Thrift Compact stream;
  `FileMetaDataReader` decodes field 7 into the list.

A union member Hardwood does not recognise (a future ordering with a field id other than 1 or 2)
decodes to `ColumnOrder.UNKNOWN`. This follows the reader's existing convention for non-fatal unknown
union/enum members — `LogicalType` skips and yields `null`, `Encoding` maps to `Encoding.UNKNOWN` —
rather than failing the file open. The decoded value is surfaced on `FileMetaData.columnOrders` so a
caller can inspect it; pruning is unaffected because a column with an unrecognised order simply falls
back to the same total-order comparison used for the recognised orders.

### Order-aware ±0 pruning

The decoded order is threaded to the pruning site so each float/double leaf prunes by its own
ordering. `FilterPredicateResolver` looks up the leaf's `ColumnOrder` and records a single
`ieee754TotalOrder` boolean on the resolved `FloatPredicate` / `Float16Predicate` / `DoublePredicate`
(`true` only for `IEEE754_TOTAL_ORDER`). `StatisticsFilterSupport.canDropFloat` / `canDropDouble` then:

- **Type-defined / absent / unrecognized order** (`ieee754TotalOrder == false`) — the spec leaves
  `±0` ambiguous (a `+0` min may hide `-0`, a `-0` max may hide `+0`), so a zero bound is widened to
  its total-order extreme (`min` ⇒ `-0.0`, `max` ⇒ `+0.0`) before the `Float.compare` checks. Widening
  only enlarges the candidate range, so it can never cause an incorrect drop.
- **IEEE 754 total order** (`ieee754TotalOrder == true`) — `-0 < +0` is unambiguous and the stored
  bounds are exact, so no widening is applied and `±0` prunes precisely.

This matches the spec, which states the `±0` compatibility rule only for the type-defined order. Real
writers (PyArrow, parquet-mr) emit the type-defined order, so they take the widening path.

The order is resolved once per reader from the first file's `column_orders` and applies to the whole
read. `column_orders` is not part of schema-compatibility validation, so in principle a sibling file
of a multi-file read could declare a different order; in practice a writer emits a uniform order
across a dataset. The only divergence that could mis-prune — a reference file declaring
`IEEE754_TOTAL_ORDER` (skip widening) while a sibling is type-defined (needs widening) — requires the
IEEE 754 total order, which no writer currently emits.

## Testing

- `ColumnOrderReaderTest` decodes the `TYPE_ORDER`, `IEEE_754_TOTAL_ORDER`, unrecognised, and empty
  union forms from raw Thrift bytes.
- `ColumnOrdersTest` asserts `column_orders` is surfaced as `TYPE_DEFINED_ORDER` for a real PyArrow
  fixture.
- `FilterPredicateResolverTest` asserts the `ieee754TotalOrder` flag is set from a leaf's
  `ColumnOrder` (type-defined / IEEE754 / absent).
- `NaNStatisticsFilterTest` asserts a type-defined `±0` bound is not dropped by a `== ∓0` predicate,
  an IEEE754 `±0` bound prunes precisely, and non-zero predicates still prune under both orders.
