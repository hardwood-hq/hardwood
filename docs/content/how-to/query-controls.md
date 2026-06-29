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

1. **Row group** — entire row groups whose statistics prove no rows can match are skipped.
2. **Page** — within surviving row groups, the Column Index (per-page min/max statistics) is used to skip individual pages, avoiding unnecessary decompression and decoding. On remote backends like S3, only the matching pages are fetched, so the same skip also reduces network I/O.
3. **Record** — `buildRowReader().filter(filter).build()` evaluates the predicate against each decoded row and returns only rows that actually match.

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

// IN filter
FilterPredicate filter = FilterPredicate.in("department_id", 1, 3, 7);
FilterPredicate filter = FilterPredicate.inStrings("city", "NYC", "LA", "Chicago");

// NULL checks
FilterPredicate filter = FilterPredicate.isNull("middle_name");
FilterPredicate filter = FilterPredicate.isNotNull("email");

try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.buildRowReader().filter(filter).build()) {

    while (rowReader.hasNext()) {
        rowReader.next();
        // Only rows from non-skipped row groups are returned
    }
}
```

For the full matrix of supported operators, comparable physical and logical types, and combinators, see [Query Controls](../reference/query-controls.md#supported-filter-predicates). All predicates, including those wrapped in `not`, are pushed down to the statistics level for row-group and page skipping.

### Null Handling

Comparison predicates (`eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq`, `in`, `inStrings`) follow SQL three-valued logic: any comparison against a null column value yields UNKNOWN, and rows whose predicate is UNKNOWN are not returned. Put differently, **rows where the tested column is null are never returned by a comparison predicate** — including `notEq`.

`not(p)` preserves this behavior: rows where `p` is UNKNOWN stay UNKNOWN under negation and are dropped. The SQL identity `not(gt(x, v)) ≡ ltEq(x, v)` holds on all rows, including null ones.

To include null rows explicitly, combine with `isNull`:

```java
// rows with age > 30, plus rows where age is null
FilterPredicate filter = FilterPredicate.or(
    FilterPredicate.gt("age", 30),
    FilterPredicate.isNull("age")
);
```

!!! note "Divergence from parquet-java"
    parquet-java's `notEq` treats `null <> v` as true and therefore includes null rows, which breaks the SQL identity above. Hardwood applies uniform SQL three-valued-logic semantics across all comparison operators. To reproduce parquet-java's behavior, make the null-inclusion explicit: `or(notEq("x", v), isNull("x"))`.

### Float and Double Comparisons

Predicates on `float` and `double` columns use the `Float.compare` / `Double.compare` total order, not IEEE 754 equality. Two consequences matter in practice:

- `-0.0` is strictly less than `+0.0`. `eq(0.0)` matches only `+0.0` values; to match either zero, use `or(eq(0.0), eq(-0.0))`.
- `NaN` sorts above every finite value. `eq(Float.NaN)` matches only `NaN` (whereas IEEE `NaN == anything` is always false). `lt` and `ltEq` against any value never match `NaN` rows; `gt` and `gtEq` against a finite value always include `NaN` rows.

```java
// Match any NaN row
FilterPredicate anyNaN = FilterPredicate.eq("score", Double.NaN);

// Match both signed zeros
FilterPredicate anyZero = FilterPredicate.or(
    FilterPredicate.eq("ratio", 0.0),
    FilterPredicate.eq("ratio", -0.0)
);
```

Row-group and page-level pushdown is defensive against non-conformant writers: if a column's statistics carry `NaN` as `min` or `max` (forbidden by the Parquet spec, but produced by older or buggy writers), that bound is treated as no-bound and the row group / page is not pruned on its account.

### Logical Type Support

Factory methods are provided for common Parquet logical types, handling the physical
encoding automatically:

```java
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

// DATE columns
FilterPredicate filter = FilterPredicate.gt("birth_date", LocalDate.of(2000, 1, 1));

// TIMESTAMP columns — time unit is resolved from the column schema
FilterPredicate filter = FilterPredicate.gtEq("created_at",
    Instant.parse("2025-01-01T00:00:00Z"));

// TIME columns
FilterPredicate filter = FilterPredicate.lt("start_time", LocalTime.of(9, 0));

// DECIMAL columns — scale and physical type are resolved from the column schema
FilterPredicate filter = FilterPredicate.gtEq("amount", new BigDecimal("99.99"));

// UUID columns — column must carry the UUID logical type
FilterPredicate filter = FilterPredicate.eq("request_id",
    UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
```

The logical-type factories validate the column's logical type at reader creation: `BigDecimal` predicates require a `DECIMAL` column and `UUID` predicates require a `UUID` column. Applying them to a plain `FIXED_LEN_BYTE_ARRAY` column without the corresponding logical-type annotation throws `IllegalArgumentException`.

Raw physical-type predicates (`int`, `long`, etc.) remain available for columns without logical types or for filtering on the underlying physical value directly.

Filters work with all reader types: `RowReader`, `ColumnReader`, `AvroRowReader`, and across multi-file readers.

### Limitations

- **Record-level filtering only applies to flat schemas
  ([#222](https://github.com/hardwood-hq/hardwood/issues/222)).** When the schema contains
  nested columns (structs, lists, or maps), record-level filtering is not active. Row-group
  and page-level statistics pushdown still apply, but non-matching rows within surviving pages
  will not be filtered out. A warning is logged when this occurs.
- **Bloom filter pushdown is not supported
  ([#105](https://github.com/hardwood-hq/hardwood/issues/105)).** Parquet files may contain
  Bloom filters for high-cardinality columns, but Hardwood does not currently use them for
  filter evaluation.
- **Dictionary-based filtering is not supported
  ([#196](https://github.com/hardwood-hq/hardwood/issues/196)).** Dictionary-encoded columns
  are not checked for predicate matches before decoding.

## Column Projection

Column projection allows reading only a subset of columns from a Parquet file, improving performance by skipping I/O, decoding, and memory allocation for unneeded columns.

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

### Combining Projection and Filters

Column projection and predicate pushdown can be used together:

```java
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.buildRowReader()
             .projection(ColumnProjection.columns("id", "name", "salary"))
             .filter(FilterPredicate.gtEq("salary", 50000L))
             .build()) {

    while (rowReader.hasNext()) {
        rowReader.next();
        long id = rowReader.getLong("id");
        String name = rowReader.getString("name");
        long salary = rowReader.getLong("salary");
    }
}
```

The filter column does not need to be in the projection — Hardwood reads the filter column's statistics for pushdown regardless.

## Row Limit

A row limit instructs the reader to stop after the specified number of rows, avoiding unnecessary I/O and decoding. On remote backends like S3, only the row groups and pages needed to satisfy the limit are fetched.

```java
// Read at most 100 rows
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.buildRowReader().head(100).build()) {

    while (rowReader.hasNext()) {
        rowReader.next();
        // At most 100 rows will be returned
    }
}
```

The row limit can be combined with column projection and filters:

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

When you partition a file across parallel readers — Flink `BulkFormat`, Spark file source, MapReduce-style splits — each reader is assigned a byte range and is responsible for only the row groups owned by it. Express this with `RowGroupPredicate.byteRange(start, end)`, passed to any builder's `filter(...)`:

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

A row group is included if and only if its **midpoint** — the start of its first column chunk plus half of its on-disk compressed size — falls in `[start, end)`. This is the standard Hadoop-input-format split convention: across a partitioning of the file into disjoint byte ranges, every row group lands in exactly one range, regardless of where the split boundary falls inside the row group itself.

**Granularity is row-group, not row.** A row group whose midpoint is in `[0, 1000)` is read in full, including any rows whose data extends beyond byte 1000. If you need true row-level windowing, combine `RowGroupPredicate` with [`RowReaderBuilder.skip(...)`](#skipping-rows-skip) and [`head(...)`](#row-limit).

`RowGroupPredicate` composes with [`FilterPredicate`](#predicate-pushdown-filter) via intersection — both apply, and a row group is read if and only if it passes both:

```java
ColumnReader col = fileReader.buildColumnReader("price")
        .filter(FilterPredicate.gt("price", 100))                  // column-stats
        .filter(RowGroupPredicate.byteRange(splitStart, splitEnd)) // layout
        .build();
```

`RowGroupPredicate.and(...)` lets you intersect multiple row-group conditions:

```java
.filter(RowGroupPredicate.and(
        RowGroupPredicate.byteRange(splitStart, splitEnd),
        RowGroupPredicate.byteRange(otherStart, otherEnd)))
```

The same `filter(RowGroupPredicate)` overload is available on `RowReaderBuilder` and `ColumnReadersBuilder`. On `RowReaderBuilder`, `skip(N)` and `head(N)` index over the *row-group-filtered* sequence — `skip(N)` skips `N` rows of the kept set, `head(N)` caps reading at `N` rows of the kept set. Combining `RowGroupPredicate` with `tail(N)` is rejected: tail mode requires a known total row count, which row-group filtering invalidates.

### Empty ranges

`byteRange(start, end)` where `end < start` is a documented empty range — the reader yields zero rows. This matches callers that pass `splitStart + splitLength` and tolerate long overflow on tail splits (a tail split with `length = Long.MAX_VALUE` overflows to a negative end, which your reader will then treat as empty if no preceding split has already covered the rest of the file).

### Reading the Tail of a File

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

Tail mode cannot currently be combined with a filter predicate — the set of matching rows is not known from row-group statistics alone, so the reader cannot identify which row groups cover the last N matching rows without scanning the whole file. It is also mutually exclusive with `skip(long)`.

### Skipping Rows (`skip`)

The `skip(long)` builder method is SQL `OFFSET`: it discards leading rows before reading. What "leading rows" means depends on whether a filter is present — see [Row Selection](../concepts/row-selection.md) for the model.

**Without a filter,** `skip(n)` is a physical absolute row index. Earlier row groups are not opened — their pages are not fetched or decoded — making this an O(1 row group) seek on remote backends, in contrast to walking `next()` from row 0.

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

`skip == 0` is the no-op default. `skip >= totalRows` produces an empty reader (a SQL `OFFSET` past the end of the file, or of the concatenated relation for multi-file readers). The seek lands at row-group granularity: rows before the offset within the target row group are still read.

For multi-file readers, physical `skip(N)` is a global offset across the input files in order. Hardwood reads the footers of skipped files to count their rows, but skipped files' data pages are not fetched or decoded.

**With a filter,** `skip(n)` is a *logical* offset over the matched rows — it discards the first `n` rows that match the predicate and returns the rest, exactly like SQL `OFFSET` after a `WHERE`. The O(1 row-group) seek does **not** apply: row-group statistics bound min/max values, not match *counts*, so the reader decodes earlier groups to count matches (groups whose statistics prove no match are still pruned). A `skip` past the number of matching rows yields an empty reader rather than throwing.

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

Compose `skip` with `head` for a bounded window — `skip(n).head(k)` returns at most `k` rows starting after the first `n` (physical rows without a filter, matched rows with one). `skip(N)` also composes with `filter(RowGroupPredicate)` (e.g. `byteRange`), indexing into the kept row-group sequence. `skip` is mutually exclusive with `tail`.

