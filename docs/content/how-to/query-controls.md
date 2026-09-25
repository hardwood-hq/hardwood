<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Predicate Pushdown, Projection, Limits, and Splits

!!! example "Try it yourself"
    Want to run it or explore the capabilities yourself? The [**Query Controls**](https://github.com/hardwood-hq/hardwood-examples/tree/main/query-controls) example filters, projects, limits, paginates, and splits a read by byte range.

## Predicate Pushdown (Filter)

Filter predicates apply at three levels, in this order:

1. **Row group** — entire row groups whose statistics prove no rows can match are skipped. For `eq` and `in` predicates, a column's [Bloom filter](https://parquet.apache.org/docs/file-format/bloomfilter/) and, when the chunk's `encoding_stats` show every data page is dictionary-encoded, the column chunk's dictionary page also skip a row group that never stored the target. Dictionary pages are read only for a row group that statistics and the Bloom filter did not already drop.
2. **Page** — within surviving row groups, the Column Index (per-page min/max statistics) is used to skip individual pages, avoiding unnecessary decompression and decoding. On remote backends like S3, only the matching pages are fetched.
3. **Record** — `buildRowReader().filter(filter).build()` evaluates the predicate against each decoded row and returns only rows that match.

In a row group whose statistics prove that every row matches, the predicate is not evaluated, and a filter column outside the projection is neither fetched nor decoded.

For spatial filtering on GEOMETRY / GEOGRAPHY columns, see [Geospatial Support](geospatial.md).

```java
import dev.hardwood.reader.FilterPredicate;

// Simple filter
FilterPredicate filter = FilterPredicate.gt("age", 21);

// Compound filter
FilterPredicate filter = FilterPredicate.and(
    FilterPredicate.gtEq("salary", 50000L),
    FilterPredicate.lt("age", 65)
);

// IN filter, taking the literal type of the column
FilterPredicate filter = FilterPredicate.in("department_id", 1, 3, 7);
FilterPredicate filter = FilterPredicate.in("temperature", 20.5, 21.0, 22.5);
FilterPredicate filter = FilterPredicate.in("city", "NYC", "LA", "Chicago");
FilterPredicate filter = FilterPredicate.in("order_date",
    LocalDate.of(2025, 1, 1), LocalDate.of(2025, 7, 1));

// Binary filter, on the bytes the column stores
FilterPredicate filter = FilterPredicate.eq("checksum", new byte[] { 0x00, (byte) 0xC8 });

// NULL checks
FilterPredicate filter = FilterPredicate.isNull("middle_name");
FilterPredicate filter = FilterPredicate.isNotNull("email");

// NULL checks on a group, testing whether the group itself is present
FilterPredicate filter = FilterPredicate.isNull("address");     // a struct
FilterPredicate filter = FilterPredicate.isNotNull("tags");     // a LIST
FilterPredicate filter = FilterPredicate.isNull("attributes");  // a MAP
```

On a group, `isNull("address")` matches rows where the `address` group is absent, and `isNotNull("address")` rows where it is present, however empty. A struct whose every field is null is present, so `isNotNull("address")` matches it — a different question from `isNotNull("address.city")`, which asks about the field. An empty list and an empty map are likewise present, so only a list or map that is itself absent matches `isNull`.

```java
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.buildRowReader().filter(filter).build()) {

    while (rowReader.hasNext()) {
        rowReader.next();
        // Only rows matching the filter are returned
    }
}
```

For the supported operators, column forms and combinators, see [Query Controls](../reference/query-controls.md#supported-filter-predicates).

### Null Handling

Comparison predicates (`eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq`, `in`) follow SQL three-valued logic: any comparison against a null column value yields UNKNOWN, and rows whose predicate is UNKNOWN are not returned. Put differently, **rows where the tested column is null are never returned by a comparison predicate**, `notEq` included, and neither by `not` over one.

To include null rows explicitly, combine with `isNull`:

```java
// rows with age > 30, plus rows where age is null
FilterPredicate filter = FilterPredicate.or(
    FilterPredicate.gt("age", 30),
    FilterPredicate.isNull("age")
);
```

parquet-java's `notEq` includes null rows. To reproduce it, write `or(notEq("x", v), isNull("x"))`. The reasoning is in [Compatibility Philosophy](../concepts/compatibility-philosophy.md#sql-three-valued-logic-for-comparison-predicates).

### Float and Double Comparisons

Predicates on `float` and `double` columns use the `Float.compare` / `Double.compare` total order (see [Predicate literals by column type](../reference/query-controls.md#predicate-literals-by-column-type)). `eq(0.0)` matches only `+0.0` values, and `eq(Double.NaN)` matches every `NaN`:

```java
// Match any NaN row
FilterPredicate anyNaN = FilterPredicate.eq("score", Double.NaN);

// Match both signed zeros
FilterPredicate anyZero = FilterPredicate.or(
    FilterPredicate.eq("ratio", 0.0),
    FilterPredicate.eq("ratio", -0.0)
);
```

Row-group and page pruning never drops a `NaN` row that a predicate matches. Which floating-point predicates prune from statistics is listed under [When statistics are ignored](../reference/query-controls.md#when-statistics-are-ignored).

### Logical Type Support

Factory methods are provided for common Parquet logical types, handling the physical
encoding automatically:

```java
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;

// DATE columns
FilterPredicate filter = FilterPredicate.gt("birth_date", LocalDate.of(2000, 1, 1));

// TIMESTAMP columns — time unit is resolved from the column schema
FilterPredicate filter = FilterPredicate.gtEq("created_at",
    Instant.parse("2025-01-01T00:00:00Z"));

// TIMESTAMP columns with isAdjustedToUTC = false hold a wall clock, and take a LocalDateTime
FilterPredicate filter = FilterPredicate.lt("pickup_time",
    LocalDateTime.of(2025, 1, 1, 8, 30));

// DECIMAL columns — scale and physical type are resolved from the column schema
FilterPredicate filter = FilterPredicate.gtEq("amount", new BigDecimal("99.99"));
```

The literal types each column takes, and the order it compares in, are listed under [Predicate literals by column type](../reference/query-controls.md#predicate-literals-by-column-type); a literal of another type throws `IllegalArgumentException` at reader creation. A literal the column cannot store follows [Literals the column cannot hold](../reference/query-controls.md#literals-the-column-cannot-hold). Which predicates the Bloom filter and dictionary sharpen is listed under [Bloom filter and dictionary pruning](../reference/query-controls.md#bloom-filter-and-dictionary-pruning).

A predicate on a column or group nested inside a `LIST` or a `MAP` has no single answer per row, and is rejected with `IllegalArgumentException` at reader creation.

## Column Projection

Column projection reads only a subset of columns from a Parquet file; unneeded columns are not fetched, decoded or allocated.

```java
import dev.hardwood.InputFile;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.buildRowReader()
             .projection(ColumnProjection.columns("id", "name", "created_at"))
             .build()) {

    while (rowReader.hasNext()) {
        rowReader.next();

        // Access projected columns normally
        long id = rowReader.getLong("id");
        String name = rowReader.getString("name");
        Instant createdAt = rowReader.getTimestamp("created_at");

        // Accessing non-projected columns throws IllegalArgumentException
        // rowReader.getInt("age");  // throws "Column not in projection: age"
    }
}
```

`ColumnProjection.all()` reads every column (the default); `ColumnProjection.columns(...)` selects by name, including a whole struct and its children or a dot-notation nested field. See [Query Controls](../reference/query-controls.md#column-projection-forms) for the full list of forms.

A filter column does not need to be in the projection. A filter column outside the projection is not a field of the row at any depth (see [Query Controls](../reference/query-controls.md#column-projection-forms) for the exceptions an accessor raises); project a column you intend to read.

## Row Limit

A row limit instructs the reader to stop after the specified number of rows. On remote backends like S3, only the row groups and pages needed to satisfy the limit are fetched. It combines with column projection and filters:

```java
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.buildRowReader()
             .projection(ColumnProjection.columns("id", "name"))
             .filter(FilterPredicate.gt("age", 21))
             .head(100)
             .build()) {

    while (rowReader.hasNext()) {
        rowReader.next();
        // At most 100 matching rows with only id and name columns
    }
}
```

When combined with a filter, the limit applies to the number of **matching** rows, not the total number of scanned rows.

## Split-Aware Reading

When you partition a file across parallel readers (Flink `BulkFormat`, Spark file source, MapReduce-style splits), each reader is assigned a byte range and is responsible for only the row groups owned by it. Express this with `RowGroupPredicate.byteRange(start, end)`, passed to any builder's `filter(...)`:

```java
import dev.hardwood.reader.RowGroupPredicate;

// One reader subtask, owning the file's first quarter.
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     ColumnReader col = fileReader.buildColumnReader("price")
             .filter(RowGroupPredicate.byteRange(splitStart, splitEnd))
             .build()) {
    while (col.nextBatch()) {
        // Only batches from row groups owned by this split.
    }
}
```

A row group is included if and only if its **midpoint**, the start of its first column chunk plus half of its on-disk compressed size, falls in `[start, end)`, the Hadoop-input-format split convention. Across a partitioning of the file into disjoint byte ranges, every row group lands in exactly one range. [Row Selection](../concepts/row-selection.md#physical-splitting-is-separate) describes how this relates to the other controls.

**Granularity is row-group, not row.** A row group whose midpoint is in `[0, 1000)` is read in full, including any rows whose data extends beyond byte 1000. For row-level windowing, combine `RowGroupPredicate` with [`skip(...)`](#skipping-rows-skip) and [`head(...)`](#row-limit).

`RowGroupPredicate` composes with [`FilterPredicate`](#predicate-pushdown-filter) via intersection: a row group is read if and only if it passes both. `RowGroupPredicate.and(...)` intersects several row-group conditions.

```java
ColumnReader col = fileReader.buildColumnReader("price")
        .filter(FilterPredicate.gt("price", 100))                  // column-stats
        .filter(RowGroupPredicate.byteRange(splitStart, splitEnd)) // layout
        .build();
```

The same `filter(RowGroupPredicate)` overload is available on `RowReaderBuilder` and `ColumnReadersBuilder`. On `RowReaderBuilder`, `skip(N)` and `head(N)` index over the *row-group-filtered* sequence. Combining `RowGroupPredicate` with `tail(N)` is rejected, since tail mode requires a known total row count.

A byte range names positions in one file, so `RowGroupPredicate` applies to a reader opened on a single file. On a reader opened with `openAll` over several files, `build()` throws `UnsupportedOperationException`; open one reader per split's file instead.

`byteRange(start, end)` where `end < start` is an empty range, for which the reader yields zero rows. A tail split whose `splitStart + splitLength` overflows `long` therefore reads nothing.

## Reading the Tail of a File

The `tail(N)` builder method reads the trailing rows of the file instead of the leading ones. Row groups that do not overlap the tail are skipped entirely, so pages for earlier row groups are never fetched or decoded.

```java
// Read the last 10 rows; earlier row groups are skipped.
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.buildRowReader().tail(10).build()) {

    while (rowReader.hasNext()) {
        rowReader.next();
        // ...
    }
}
```

Tail mode cannot be combined with a filter predicate (see [Supported combinations](../concepts/row-selection.md#supported-combinations)) or with `skip(long)`.

## Skipping Rows (`skip`)

The `skip(long)` builder method is SQL `OFFSET`: it discards leading rows before reading. What "leading rows" means depends on whether a filter is present; see [Row Selection](../concepts/row-selection.md) for the model.

**Without a filter,** `skip(n)` is a physical absolute row index. Earlier row groups are not opened, and their pages are not fetched or decoded.

```java
// Read rows starting at row 1,000,000 — earlier row groups are not opened.
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.buildRowReader().skip(1_000_000).build()) {

    while (rowReader.hasNext()) {
        rowReader.next();
        // ...
    }
}
```

`skip == 0` is the no-op default. `skip >= totalRows` produces an empty reader. The seek lands at row-group granularity: rows before the offset within the target row group are read and discarded.

For multi-file readers, physical `skip(N)` is a global offset across the input files in order. Hardwood reads the footers of skipped files to count their rows, but skipped files' data pages are not fetched or decoded.

**With a filter,** `skip(n)` discards the first `n` rows that match the predicate. The reader decodes earlier row groups to count matches, pruning only those whose statistics prove no match. A `skip` past the number of matching rows yields an empty reader rather than throwing.

```java
// Skip the first 150 rows matching the predicate, then read the next 20.
// OFFSET 150 LIMIT 20 over the filtered relation.
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.buildRowReader()
             .filter(FilterPredicate.gt("age", 21))
             .skip(150)
             .head(20)
             .build()) {
    // ...
}
```
