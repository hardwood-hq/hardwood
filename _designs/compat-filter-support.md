# Design: Filter support in parquet-java-compat

**Status: Implemented**

## Goal

Support the upstream parquet-java filter idiom:

```java
import static org.apache.parquet.filter2.predicate.FilterApi.*;

FilterPredicate pred = and(
    gtEq(longColumn("id"), 2L),
    eq(intColumn("status"), 1)
);

ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
    .withFilter(FilterCompat.get(pred))
    .build();
```

## Upstream API surface

The filter API spans several classes:

1. **`FilterApi`** — static factory for column selectors (`intColumn`, `longColumn`, ...) and predicates (`eq`, `gt`, `and`, ...)
2. **`FilterPredicate`** — sealed interface, base for all predicate nodes
3. **`Operators`** — column types (`IntColumn`, `LongColumn`, ...) and predicate records (`Eq`, `Lt`, ...)
4. **`FilterCompat`** — adapter that wraps `FilterPredicate` into a `Filter` for the builder
5. **`ParquetReader.Builder.withFilter(Filter)`** — wires the filter into the reader

## Mapping to Hardwood

Hardwood's `dev.hardwood.reader.FilterPredicate` supports: `EQ`, `NOT_EQ`, `LT`, `LT_EQ`, `GT`, `GT_EQ`, `AND`, `OR`, `NOT` — with typed leaf predicates per primitive type. The compat layer translates the parquet-java predicate tree to a Hardwood `FilterPredicate`.

Unsupported upstream predicates (`IN`, `NOT_IN`, `CONTAINS`, `USER_DEFINED`) throw `UnsupportedOperationException` with a clear message.

## New shim classes

| File | Upstream package | Notes |
|------|-----------------|-------|
| `FilterApi.java` | `o.a.p.filter2.predicate` | Static factory methods |
| `FilterPredicate.java` | `o.a.p.filter2.predicate` | Marker interface |
| `Operators.java` | `o.a.p.filter2.predicate` | Column types + predicate records |
| `FilterCompat.java` | `o.a.p.filter2.compat` | `get(FilterPredicate)` → `Filter` wrapper |

## Modified classes

| File | Change |
|------|--------|
| `ParquetReader.Builder` | Add `withFilter(FilterCompat.Filter)`, wire to `createRowReader(filter)` |

## Translation

The `FilterCompat.get()` call stores the parquet-java `FilterPredicate`. When `ParquetReader.build()` is called, the builder walks the predicate tree and translates each node to its Hardwood equivalent:

- `Operators.Eq<Integer>(IntColumn("x"), 5)` → `dev.hardwood.reader.FilterPredicate.eq("x", 5)`
- `Operators.Gt<Long>(LongColumn("x"), 10L)` → `dev.hardwood.reader.FilterPredicate.gt("x", 10L)`
- `Operators.And(left, right)` → `dev.hardwood.reader.FilterPredicate.and(left', right')`
- etc.

The translation lives in a package-private `FilterConverter` class in `o.a.p.hadoop.util` (same package as `InputFiles`).

## Testing

Add filter tests to `ParquetReaderCompatTest` using the exact upstream idiom. No new test file needed.
