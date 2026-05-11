<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Usage

This page describes how to read Parquet files with Hardwood: choosing a reader, row-oriented reading, predicate pushdown, column projection, row limits, split-aware reading, column-oriented (batch) reading, reading multiple files as one dataset, accessing file metadata, and working with Variant and geospatial columns.

For detailed class-level documentation, see the [JavaDoc](/api/latest/).

## Choosing a Reader

Hardwood provides two reader APIs:

- **`RowReader`** — row-oriented access with typed getters, including nested structs, lists, and maps. Best for general-purpose reading where you process one row at a time.
- **`ColumnReader`** — batch-oriented columnar access with typed primitive arrays. Best for analytical workloads where you process columns independently (e.g. summing a column, computing statistics).

Both support column projection and predicate pushdown. Each reader has a no-arg shortcut for default reads and a builder form for filtered or limited reads:

| Reader | Shortcut | Builder |
|--------|----------|---------|
| `RowReader` | `reader.rowReader()` | `reader.buildRowReader().…build()` |
| `ColumnReader` (single) | `reader.columnReader("id")` | `reader.buildColumnReader("id").…build()` |
| `ColumnReaders` (multiple) | `reader.columnReaders(projection)` | `reader.buildColumnReaders(projection).…build()` |

To read multiple files as a single dataset with cross-file prefetching, open the `ParquetFileReader` with a list of `InputFile`s via the `Hardwood` class.

## Row-Oriented Reading

The `RowReader` provides a convenient row-oriented interface for reading Parquet files with typed accessor methods for type-safe field access.

```java
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqMap;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.math.BigDecimal;
import java.util.UUID;

try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
    RowReader rowReader = fileReader.rowReader()) {

    while (rowReader.hasNext()) {
        rowReader.next();

        // Access columns by name with typed accessors
        long id = rowReader.getLong("id");
        String name = rowReader.getString("name");

        // Logical types are automatically converted
        LocalDate birthDate = rowReader.getDate("birth_date");
        Instant createdAt = rowReader.getTimestamp("created_at");
        LocalTime wakeTime = rowReader.getTime("wake_time");
        BigDecimal balance = rowReader.getDecimal("balance");
        UUID accountId = rowReader.getUuid("account_id");

        // Check for null values
        if (!rowReader.isNull("age")) {
            int age = rowReader.getInt("age");
            System.out.println("ID: " + id + ", Name: " + name + ", Age: " + age);
        }

        // Access nested structs
        PqStruct address = rowReader.getStruct("address");
        if (address != null) {
            String city = address.getString("city");
            int zip = address.getInt("zip");
        }

        // Access lists and iterate with typed accessors
        PqList tags = rowReader.getList("tags");
        if (tags != null) {
            for (String tag : tags.strings()) {
                System.out.println("Tag: " + tag);
            }
        }
    }
}
```

??? note "Advanced: nested lists, maps, and list-of-structs"

    ```java
            // Access list of structs
            PqList contacts = rowReader.getList("contacts");
            if (contacts != null) {
                for (PqStruct contact : contacts.structs()) {
                    String contactName = contact.getString("name");
                    String phone = contact.getString("phone");
                }
            }

            // Access nested lists (list<list<int>>) using primitive int lists
            PqList matrix = rowReader.getList("matrix");
            if (matrix != null) {
                for (PqIntList innerList : matrix.intLists()) {
                    for (var it = innerList.iterator(); it.hasNext(); ) {
                        int val = it.nextInt();
                        System.out.println("Value: " + val);
                    }
                }
            }

            // Access maps (map<string, int>) — iterate all entries
            PqMap attributes = rowReader.getMap("attributes");
            if (attributes != null) {
                for (PqMap.Entry entry : attributes.getEntries()) {
                    String key = entry.getStringKey();
                    int value = entry.getIntValue();
                    System.out.println(key + " = " + value);
                }
            }

            // Key-based lookup (no per-entry flyweight allocations)
            PqMap attrs = rowReader.getMap("attributes");
            if (attrs != null && attrs.containsKey("age")) {
                Integer age = (Integer) attrs.getValue("age");
            }

            // Access maps with struct values (map<string, struct>)
            PqMap people = rowReader.getMap("people");
            if (people != null) {
                PqStruct alice = (PqStruct) people.getValue("alice");
                if (alice != null) {
                    String name = alice.getString("name");
                    int age = alice.getInt("age");
                }
            }
    ```

    `PqMap.getValue(key)` returns `null` for both an absent key and a
    present-but-null value — call `containsKey(key)` to disambiguate.
    Lookup is supported by `String` / `int` / `long` / `byte[]` keys;
    long-tail key types (DATE / TIMESTAMP / DECIMAL / UUID) are reachable
    through `getEntries()` + `Entry.getKey()`. Parquet permits duplicate
    keys; the lookup methods walk in entry order and surface the first
    match.

### Typed Accessor Methods

All accessor methods are available in two forms:

- **Name-based** (e.g., `getInt("column_name")`) — convenient for ad-hoc access
- **Index-based** (e.g., `getInt(columnIndex)`) — faster for performance-critical loops

| Method | Physical Type | Logical Type | Java Type |
|--------|--------------|-------------|-----------|
| `getBoolean` | BOOLEAN | | `boolean` |
| `getInt` | INT32 | | `int` |
| `getLong` | INT64 | | `long` |
| `getFloat` | FLOAT, or FIXED_LEN_BYTE_ARRAY(2) | FLOAT16 (optional) | `float` |
| `getDouble` | DOUBLE | | `double` |
| `getBinary` | BYTE_ARRAY | BSON (optional) | `byte[]` |
| `getString` | BYTE_ARRAY | STRING or JSON | `String` |
| `getDate` | INT32 | DATE | `LocalDate` |
| `getTime` | INT32 or INT64 | TIME | `LocalTime` |
| `getTimestamp` | INT64, or legacy INT96 | TIMESTAMP | `Instant` |
| `getDecimal` | INT32, INT64, or FIXED_LEN_BYTE_ARRAY | DECIMAL | `BigDecimal` |
| `getUuid` | FIXED_LEN_BYTE_ARRAY | UUID | `UUID` |
| `getInterval` | FIXED_LEN_BYTE_ARRAY(12) | INTERVAL | `PqInterval` |
| `getStruct` | | | `PqStruct` |
| `getList` | | LIST | `PqList` |
| `getMap` | | MAP | `PqMap` |
| `getVariant` | BYTE_ARRAY pair | VARIANT | `PqVariant` |
| `isNull` | Any | Any | `boolean` |

All methods are available as both `method(name)` and `method(index)`, except `getStruct`, `getList`, `getMap`, and `getVariant` which are name-based only.

#### Null handling

Primitive accessors (`getInt`, `getLong`, `getFloat`, `getDouble`, `getBoolean`) throw `NullPointerException` if the field is null — always check with `isNull()` first. Object accessors (`getString`, `getDate`, `getTimestamp`, `getDecimal`, `getUuid`, `getInterval`, `getStruct`, `getList`, `getMap`) return `null` for null fields.

#### Type validation

The API validates at runtime that the requested type matches the schema. Mismatches throw `IllegalArgumentException` with a descriptive message.

#### Index-based access

For hot loops, look up column indices once outside the loop and pass them to the accessors instead of names:

```java
// Get column indices once (before the loop)
int idIndex = fileReader.getFileSchema().getColumn("id").columnIndex();
int nameIndex = fileReader.getFileSchema().getColumn("name").columnIndex();

while (rowReader.hasNext()) {
    rowReader.next();
    if (!rowReader.isNull(idIndex)) {
        long id = rowReader.getLong(idIndex);      // No name lookup per row
        String name = rowReader.getString(nameIndex);
    }
}
```

#### INTERVAL columns

`PqInterval` is a plain record with three `long` properties — `months()`, `days()`, and `milliseconds()`. Each holds an unsigned 32-bit value in the range `[0, 4_294_967_295]`, so no additional conversion is needed. The components are independent and not normalized. Files written by older parquet-mr / Spark / Hive writers that set only the legacy `converted_type=INTERVAL` annotation are handled transparently — no caller-side opt-in is required.

#### FLOAT16 columns

`getFloat` accepts FLOAT16 columns (`FIXED_LEN_BYTE_ARRAY(2)` annotated with the `FLOAT16` logical type) and decodes the 2-byte IEEE 754 half-precision payload to a single-precision `float`. The widening is lossless — half-precision NaN, ±Infinity, and signed zero round-trip cleanly, and the original NaN bit pattern is preserved (the Parquet spec does not canonicalize NaNs on write). Use `Float.isNaN(value)` for NaN checks rather than equality. As with all primitive accessors, `isNull()` must be checked before `getFloat()` since FLOAT16 columns can be optional.

#### Legacy INT96 timestamps

Parquet files written by older versions of Apache Spark and Hive store timestamps in the deprecated INT96 physical type without a TIMESTAMP logical type annotation. `getTimestamp` detects INT96 automatically and decodes it to an `Instant`; no caller-side handling is required.

#### Bare `BYTE_ARRAY` columns

`BYTE_ARRAY` columns without a `STRING` logical type annotation may hold arbitrary binary payloads (Protobuf, WKB, custom encodings). Generic accessors such as `PqList.get` and `PqList.iterator` surface these as `byte[]` rather than silently UTF-8 decoding them — invalid byte sequences would otherwise be replaced with `U+FFFD`. Call `getString` explicitly when the column is known to contain UTF-8 text from an older writer that omitted the `STRING` annotation.

#### Typed accessors on `PqList` and `PqMap.Entry`

Both interfaces mirror the RowReader's typed accessor surface — `strings()` / `dates()` / `times()` / `timestamps()` / `decimals()` / `uuids()` / `intervals()` / `floats()` / `booleans()` on `PqList` (each returning `List<T>`); the matching `getStringValue()` / `getDateValue()` / `getIntervalValue()` / etc. on `PqMap.Entry`. Use these in preference to the generic `getValue()` when iterating over a list / map of a known logical type to avoid the boxed `Object` return.

`PqMap.Entry`'s typed *key* accessor surface is intentionally narrower: `getStringKey()` / `getIntKey()` / `getLongKey()` / `getBinaryKey()` cover the four high-frequency map key types. Long-tail key types (DATE / TIME / TIMESTAMP / DECIMAL / UUID) fall through to `getKey()` (decoded) and `getRawKey()` (raw).

`PqList.ints()` / `longs()` / `doubles()` return the specialized `PqIntList` / `PqLongList` / `PqDoubleList` types instead — these expose `PrimitiveIterator.OfInt` / `OfLong` / `OfDouble`, `int get(int)`, and `int[] toArray()` so primitive list iteration allocates no boxed wrappers. For nested `list<list<int>>` (or `<long>` / `<double>`), use `intLists()` / `longLists()` / `doubleLists()` to surface the inner lists as `PqIntList` / `PqLongList` / `PqDoubleList`.

#### Reading the physical value

When you want the raw physical value rather than the decoded logical-type representation — e.g. the INT64 micros backing a `TIMESTAMP`, the INT32 days backing a `DATE`, or the unscaled INT32 / INT64 / `byte[]` backing a `DECIMAL` — call the **typed primitive accessor that matches the column's physical type**:

```java
// TIMESTAMP column backed by INT64 micros
long micros = rowReader.getLong("created_at");

// DATE column backed by INT32 days since epoch
int daysSinceEpoch = rowReader.getInt("birth_date");

// DECIMAL(precision, scale) column backed by INT64
long unscaled = rowReader.getLong("amount");
```

`getInt` / `getLong` / `getFloat` / `getDouble` / `getBoolean` / `getBinary` accept any column whose physical type matches, regardless of the logical-type annotation — they read the underlying value directly. Use this whenever you already know the column's physical encoding and want to skip logical-type decoding.

#### Decoded generic access

When the column type isn't known ahead of time — e.g. generic projection-driven readers, dump tools, schema-introspecting frameworks — the generic fallback accessors return values decoded to their logical-type representation:

- `RowReader.getValue(name)` / `getValue(index)` — `Integer` / `Long` / `String` / `LocalDate` / `LocalTime` / `Instant` / `BigDecimal` / `UUID` / `PqInterval` / `PqVariant` / nested `PqStruct` / `PqList` / `PqMap`, with `byte[]` for un-annotated `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` columns.
- `PqStruct.getValue(name)` — same decoded mapping for nested struct fields.
- `PqMap.Entry.getKey()` / `getValue()` — same decoded mapping for map keys and values.
- `PqList.get(index)` / `PqList.values()` — same decoded mapping for list elements.

A parallel `getRawValue` family (`RowReader.getRawValue`, `PqStruct.getRawValue`, `PqMap.Entry.getRawKey` / `getRawValue`, `PqList.getRaw` / `rawValues`) returns the boxed physical value when even the physical type isn't known statically. In hot loops, prefer the typed primitive accessor described above — it avoids the boxing and the dispatch overhead.

Nested groups (struct / list / map / variant) have no distinct "raw" form and are returned through their typed flyweight (`PqStruct` / `PqList` / `PqMap` / `PqVariant`) in both modes.

## Predicate Pushdown (Filter)

Filter predicates apply at four levels, in this order:

1. **Row group** — entire row groups whose statistics prove no rows can match are skipped.
2. **Page** — within surviving row groups, the Column Index (per-page min/max statistics) is used to skip individual pages, avoiding unnecessary decompression and decoding.
3. **Network** — on remote backends like S3, only the matching pages are fetched, reducing network I/O.
4. **Record** — `buildRowReader().filter(filter).build()` evaluates the predicate against each decoded row and returns only rows that actually match.

For spatial filtering on GEOMETRY / GEOGRAPHY columns, see [Geospatial Support](#geospatial-support).

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

| Category | Supported |
|---|---|
| Comparison operators | `eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq` |
| Set operators | `in` (int, long), `inStrings` |
| Null operators | `isNull`, `isNotNull` (any type) |
| Physical types (comparison) | `int`, `long`, `float`, `double`, `boolean`, `String` |
| Logical types (comparison) | `LocalDate`, `Instant`, `LocalTime`, `BigDecimal`, `UUID` |
| Combinators | `and`, `or`, `not` (`and` / `or` accept varargs for three or more conditions) |

All predicates, including those wrapped in `not`, are pushed down to the statistics level for row-group and page skipping.

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

**Projection options:**

| Form | Description |
|------|-------------|
| `ColumnProjection.all()` | Read all columns (default) |
| `ColumnProjection.columns("id", "name")` | Read specific columns by name |
| `ColumnProjection.columns("address")` | Select an entire struct and all its children |
| `ColumnProjection.columns("address.city")` | Select a specific nested field (dot notation) |

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

A row limit instructs the reader to stop after the specified number of rows, avoiding unnecessary I/O and decoding. On remote backends like S3, this can reduce network transfers significantly — only the row groups and pages needed to satisfy the limit are fetched.

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

**Granularity is row-group, not row.** A row group whose midpoint is in `[0, 1000)` is read in full, including any rows whose data extends beyond byte 1000. If you need true row-level windowing, combine `RowGroupPredicate` with [`RowReaderBuilder.firstRow(...)`](#seeking-to-an-absolute-row) and [`head(...)`](#row-limit).

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

The same `filter(RowGroupPredicate)` overload is available on `RowReaderBuilder` and `ColumnReadersBuilder`. On `RowReaderBuilder`, `firstRow(N)` and `head(N)` index over the *row-group-filtered* sequence — `firstRow(N)` skips `N` rows of the kept set, `head(N)` caps reading at `N` rows of the kept set. Combining `RowGroupPredicate` with `tail(N)` is rejected: tail mode requires a known total row count, which row-group filtering invalidates.

### Empty ranges

`byteRange(start, end)` where `end < start` is a documented empty range — the reader yields zero rows. This matches callers that pass `splitStart + splitLength` and tolerate long overflow on tail splits (a tail split with `length = Long.MAX_VALUE` overflows to a negative end, which your reader will then treat as empty if no preceding split has already covered the rest of the file).

### Reading the Tail of a File

The `tail(N)` builder method reads the trailing rows of the file instead of the leading ones. Row groups that do not overlap the tail are skipped entirely, so pages for earlier row groups are never fetched or decoded — especially useful on remote backends like S3, where unneeded row groups avoid HTTP range requests altogether.

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

Tail mode cannot currently be combined with a filter predicate — the set of matching rows is not known from row-group statistics alone, so the reader cannot identify which row groups cover the last N matching rows without scanning the whole file. It is also mutually exclusive with `firstRow(long)`.

### Seeking to an Absolute Row

The `firstRow(long)` builder method begins iteration at an arbitrary absolute row index. Earlier row groups are not opened — their pages are not fetched or decoded — making this an O(1 row group) seek on remote backends, in contrast to walking `next()` from row 0.

```java
// Read rows starting at row 1,000,000 — earlier row groups are skipped.
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.buildRowReader().firstRow(1_000_000).build()) {

    while (rowReader.hasNext()) {
        rowReader.next();
        // ...
    }
}
```

Compose with `head(N)` for a bounded `[firstRow, firstRow + N)` window:

```java
// Read rows [1_000_000, 1_000_050).
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
     RowReader rowReader = fileReader.buildRowReader()
             .firstRow(1_000_000)
             .head(50)
             .build()) {
    // ...
}
```

`firstRow == 0` is the no-op default. `firstRow == totalRows` produces an empty reader; `firstRow > totalRows` throws `IllegalArgumentException`. Mutually exclusive with `tail(N)`.

!!! warning "Multi-file readers: first file only"
    For multi-file readers, `firstRow(N)` indexes into the **first** file's rows only — it does not seek across file boundaries. To skip whole files, omit them from the input list; to skip within a non-first file, open it separately.

Within the target row group, the reader still decodes the leading residue rows and discards them via `next()`. Page-level skip via the OffsetIndex is tracked separately.

## Column-Oriented Reading (ColumnReader)

The `ColumnReader` provides batch-oriented columnar access with typed primitive arrays, avoiding per-row method calls and boxing. This is the fastest way to consume Parquet data when you process columns independently.

!!! warning "Experimental API"
    The `ColumnReader` is under active development; the shape of the batch accessors and layer representation may change in future releases without prior deprecation.

### Reading a Single Column

```java
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ColumnReader;

try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
    // Create a column reader by name (spans all row groups automatically)
    try (ColumnReader fare = reader.columnReader("fare_amount")) {
        double sum = 0;
        while (fare.nextBatch()) {
            int count = fare.getRecordCount();
            double[] values = fare.getDoubles();
            Validity validity = fare.getLeafValidity();
            boolean hasNulls = validity.hasNulls();

            for (int i = 0; i < count; i++) {
                if (!hasNulls || validity.isNotNull(i)) {
                    sum += values[i];
                }
            }
        }
    }
}
```

The [Validity](/api/latest/dev/hardwood/reader/Validity.html) type wraps the underlying null bitmap behind `isNull(i)` / `isNotNull(i)` / `hasNulls()`. When no item in a batch is null, `getLeafValidity()` (and `getLayerValidity(k)`) returns the shared `Validity.NO_NULLS` singleton — `hasNulls()` returns `false` in O(1) and gates the no-per-element-check fast path, no per-batch allocation. Hot inner loops should hoist `hasNulls()` into a local boolean before iterating; see [Hot loops](#hot-loops-hoist-hasnulls-outside-the-loop) for why.

Typed accessors are available for each fixed-width physical type: `getInts()`, `getLongs()`, `getFloats()`, `getDoubles()`, `getBooleans()`. For varlength leaves (`BINARY`, `FIXED_LEN_BYTE_ARRAY`, `INT96`) the primary accessors are `getBinaryValues()` (a `byte[]` buffer) plus `getBinaryOffsets()` (a sentinel-suffixed `int[]` of length `getValueCount() + 1`); the byte slice for value `i` is `[offsets[i], offsets[i+1])`. The convenience accessors `getBinaries()` and `getStrings()` materialise one `byte[]` or `String` per leaf — useful for low-volume / debug paths but allocate per-row, so hot loops should read the buffers directly.

Column readers can also be created by index via `columnReader(int columnIndex)`. To attach a filter, use the builder form: `reader.buildColumnReader("id").filter(predicate).build()`.

### Reading Multiple Columns

For reading multiple columns together, use `columnReaders(projection)` which returns a `ColumnReaders` collection. Drive every reader in lockstep with `ColumnReaders.nextBatch()`:

```java
import dev.hardwood.Hardwood;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.schema.ColumnProjection;

try (ParquetFileReader parquet = ParquetFileReader.open(InputFile.of(path));
     ColumnReaders columns = parquet.buildColumnReaders(
             ColumnProjection.columns("passenger_count", "trip_distance", "fare_amount"))
             .build()) {

    long passengerCount = 0;
    double tripDistance = 0, fareAmount = 0;

    while (columns.nextBatch()) {
        int count = columns.getRecordCount();
        double[] v0 = columns.getColumnReader("passenger_count").getDoubles();
        double[] v1 = columns.getColumnReader("trip_distance").getDoubles();
        double[] v2 = columns.getColumnReader("fare_amount").getDoubles();

        for (int i = 0; i < count; i++) {
            passengerCount += (long) v0[i];
            tripDistance += v1[i];
            fareAmount += v2[i];
        }
    }
}
```

`ColumnReaders.nextBatch()` advances every underlying reader once and returns `false` when any reader is exhausted — partial advancement isn't possible because all readers consume from a shared `RowGroupIterator`. The aligned record count is exposed via `ColumnReaders.getRecordCount()`. As a defensive guard, mismatched per-reader record counts throw `IllegalStateException`. Single-column consumers, or callers that need fine-grained per-reader cadence, can still call `ColumnReader.nextBatch()` directly on the readers returned by `getColumnReader(...)`.

### Nested and Repeated Columns

#### Navigating nested groups in the schema

Before opening a reader for a nested column, you usually need to walk the file's schema tree to find the leaf you want. `SchemaNode.GroupNode` exposes the structural primitives:

- `isList()` / `isMap()` / `isStruct()` — disambiguate which kind of group a node is.
- `getListElement()` — for LIST groups, returns the element node, applying Parquet's backward-compatibility rules for legacy 2-level encodings.
- `getMapKey()` / `getMapValue()` — for MAP groups, returns the key and value nodes from the standard `map.key_value.key` / `map.key_value.value` encoding.
- `children()` — for plain struct groups, iterate to get each field.

All three navigation methods return `null` when the group isn't of the expected kind or its encoding is malformed. Callers decide whether `null` is fatal at their layer.

#### Reading nested data: the layer model

`ColumnReader` exposes a column's schema chain as a sequence of **layers**. Each non-leaf node along the chain contributes zero or one layer:

| Schema node | Contributes layer? | Layer kind |
|---|---|---|
| `REQUIRED` group | no | — |
| `OPTIONAL` group (struct) | yes | `STRUCT` |
| `LIST` / `MAP`-annotated group | yes — exactly one | `REPEATED` |

Layers are numbered `0..getLayerCount() - 1` outermost-to-innermost, and the leaf is queried separately. A flat column reports `getLayerCount() == 0`.

For each layer, two buffers describe the items at that layer:

- `getLayerValidity(k)` — a [Validity](/api/latest/dev/hardwood/reader/Validity.html) indexed by item position; `isNull(i)` / `isNotNull(i)` answer the per-item question, `hasNulls()` is the O(1) fast-path gate. Returns the shared `Validity.NO_NULLS` singleton when no item at layer `k` is null in this batch.
- `getLayerOffsets(k)` — sentinel-suffixed offsets of length `count(k) + 1` into the next inner layer's items (or, for the innermost layer, into the leaf-value array). Only valid when `getLayerKind(k) == REPEATED`; throws on a `STRUCT` layer.

The leaf has its own `getLeafValidity()` (also a `Validity`), and the four states of a `LIST`/`MAP` container fall out cleanly from offsets and validity:

| Logical value | `getLayerValidity` | `offsets[r+1] - offsets[r]` |
|---|---|---|
| `null`           | `isNull(r)`    | `0` |
| `[]`             | `isNotNull(r)` | `0` |
| `[null]`         | `isNotNull(r)` | `1`, leaf validity says null |
| `[v]`            | `isNotNull(r)` | `1`, leaf validity says not null |

Empty-vs-null is the offsets diff; the validity bit picks out null. No empty-marker bitmap is needed.

A quick reference for what `getLayerCount()` / `getLayerKind(k)` report for common chains:

| Schema chain | `getLayerCount()` | Kinds (outer → inner) |
|---|---|---|
| `optional double x` | 0 | — |
| `optional struct { ... int x }` | 1 | STRUCT |
| `list<int>` | 1 | REPEATED |
| `map<string, int>` | 1 | REPEATED |
| `list<list<int>>` | 2 | REPEATED, REPEATED |
| `optional struct { list<int> }` | 2 | STRUCT, REPEATED |
| `list<optional struct { ... }>` | 2 | REPEATED, STRUCT |
| `optional struct { map<string, int> }` | 2 | STRUCT, REPEATED |

Two rules generate this table:

1. **STRUCT keeps cardinality.** Items at layer `k+1` equal items at layer `k`. STRUCT layers carry validity, no offsets.
2. **REPEATED expands cardinality.** Items at layer `k+1` equal `getLayerOffsets(k)[count(k)]`. REPEATED layers carry both validity and offsets.

#### Hot loops: hoist `hasNulls()` outside the loop

`Validity.NO_NULLS` is the common case on analytical workloads — most columns are non-null in most batches — and the API is designed to make checking for it O(1). In a per-element inner loop, **call `hasNulls()` once outside the loop and use a local boolean inside**, rather than calling `isNotNull(i)` directly per element:

```java
Validity validity = col.getLeafValidity();
boolean hasNulls = validity.hasNulls();
for (int i = 0; i < count; i++) {
    if (!hasNulls || validity.isNotNull(i)) {
        sum += values[i];
    }
}
```

Extracting the check once per batch is meaningfully faster than calling it per element on no-nulls data, which is the common analytical-workload case. Examples below show the hoist applied; for cold paths (small batches, schema introspection, debug code) the direct `isNull(i)` / `isNotNull(i)` form is fine and reads better.

#### Flat column

```java
try (ColumnReader fare = reader.columnReader("fare_amount")) {
    while (fare.nextBatch()) {
        int count = fare.getRecordCount();
        double[] values = fare.getDoubles();
        Validity validity = fare.getLeafValidity();
        boolean hasNulls = validity.hasNulls();
        for (int i = 0; i < count; i++) {
            if (!hasNulls || validity.isNotNull(i)) {
                sum += values[i];
            }
        }
    }
}
```

#### Optional struct above an optional leaf

For `optional group customer { optional int32 age }`, the leaf `age` has one `STRUCT` layer above it. The two sources of "absent" — `customer == null` versus `customer.age == null` — show up on different bitmaps:

```java
try (ColumnReader col = reader.columnReader("customer.age")) {
    while (col.nextBatch()) {
        int recordCount = col.getRecordCount();
        Validity structValidity = col.getLayerValidity(0);  // customer null?
        Validity leafValidity   = col.getLeafValidity();    // age null (within a present customer)?
        boolean structHasNulls = structValidity.hasNulls();
        boolean leafHasNulls   = leafValidity.hasNulls();
        int[] ages = col.getInts();

        for (int r = 0; r < recordCount; r++) {
            if (structHasNulls && structValidity.isNull(r)) {
                // customer == null
            } else if (leafHasNulls && leafValidity.isNull(r)) {
                // customer != null, age == null
            } else {
                sumAge += ages[r];
            }
        }
    }
}
```

#### Simple list

For `list<double> fare_components`:

```java
try (ColumnReader col = reader.columnReader("fare_components.list.element")) {
    while (col.nextBatch()) {
        int recordCount = col.getRecordCount();
        double[] values = col.getDoubles();
        int[] offsets = col.getLayerOffsets(0);          // length recordCount + 1
        Validity listValidity = col.getLayerValidity(0);
        Validity leafValidity = col.getLeafValidity();
        boolean listHasNulls = listValidity.hasNulls();
        boolean leafHasNulls = leafValidity.hasNulls();

        for (int r = 0; r < recordCount; r++) {
            if (listHasNulls && listValidity.isNull(r)) continue;        // null list
            int start = offsets[r];
            int end   = offsets[r + 1];
            if (start == end) continue;                                  // empty list
            for (int i = start; i < end; i++) {
                if (!leafHasNulls || leafValidity.isNotNull(i)) {
                    sum += values[i];
                }
            }
        }
    }
}
```

The sentinel suffix on `offsets` removes the last-record special case from the inner loop bounds.

#### Multi-Level Nesting

For `list<list<int>>` (`getLayerCount() == 2`, both layers `REPEATED`), layer 0's offsets index into layer 1's offsets, which in turn index into the leaf array. The pattern generalises to arbitrary depth:

```java
try (ColumnReader col = reader.columnReader("matrix.list.element.list.element")) {
    while (col.nextBatch()) {
        int recordCount = col.getRecordCount();
        int[] outerOffsets     = col.getLayerOffsets(0);   // length recordCount + 1
        int[] innerOffsets     = col.getLayerOffsets(1);
        Validity outerValidity = col.getLayerValidity(0);
        Validity innerValidity = col.getLayerValidity(1);
        Validity leafValidity  = col.getLeafValidity();
        boolean outerHasNulls = outerValidity.hasNulls();
        boolean innerHasNulls = innerValidity.hasNulls();
        boolean leafHasNulls  = leafValidity.hasNulls();
        int[] values           = col.getInts();

        for (int r = 0; r < recordCount; r++) {
            if (outerHasNulls && outerValidity.isNull(r)) continue;
            int innerStart = outerOffsets[r];
            int innerEnd   = outerOffsets[r + 1];
            for (int j = innerStart; j < innerEnd; j++) {
                if (innerHasNulls && innerValidity.isNull(j)) continue;
                int valStart = innerOffsets[j];
                int valEnd   = innerOffsets[j + 1];
                for (int i = valStart; i < valEnd; i++) {
                    if (!leafHasNulls || leafValidity.isNotNull(i)) {
                        sum += values[i];
                    }
                }
            }
        }
    }
}
```

#### List of strings

Layer offsets and binary offsets are orthogonal axes: layer offsets walk which leaf values belong to a record (across the `getValueCount()` axis); binary offsets walk byte spans within a single varlength leaf (across the byte axis of the values buffer).

```java
try (ColumnReader col = reader.columnReader("tags.list.element")) {
    while (col.nextBatch()) {
        int recordCount = col.getRecordCount();
        int[] layerOffsets     = col.getLayerOffsets(0);
        Validity listValidity  = col.getLayerValidity(0);
        byte[] bytes           = col.getBinaryValues();    // capacity-sized
        int[] binaryOffsets    = col.getBinaryOffsets();   // length valueCount + 1
        Validity leafValidity  = col.getLeafValidity();
        boolean listHasNulls = listValidity.hasNulls();
        boolean leafHasNulls = leafValidity.hasNulls();

        for (int r = 0; r < recordCount; r++) {
            if (listHasNulls && listValidity.isNull(r)) continue;
            int firstValue = layerOffsets[r];
            int lastValue  = layerOffsets[r + 1];
            for (int i = firstValue; i < lastValue; i++) {
                if (leafHasNulls && leafValidity.isNull(i)) continue;
                int byteStart = binaryOffsets[i];
                int byteLen   = binaryOffsets[i + 1] - byteStart;
                if (matches(bytes, byteStart, byteLen)) hits++;
            }
        }
    }
}
```

#### Map

Maps report as `REPEATED` (Hardwood does not distinguish map-shape from list-shape on the layer enum — consult `getColumnSchema()` if you need that distinction).

To pair keys with values, open both leaves and drive them in lockstep. The two columns share the same `map.key_value` parent, so their layer offsets agree — entry `i` of one is entry `i` of the other:

```java
try (ColumnReader keys   = reader.columnReader("tags.key_value.key");
     ColumnReader values = reader.columnReader("tags.key_value.value")) {

    while (keys.nextBatch() & values.nextBatch()) {
        int recordCount        = keys.getRecordCount();
        int[]    entryOffsets  = keys.getLayerOffsets(0);
        Validity mapValidity   = keys.getLayerValidity(0);
        byte[]   keyBytes      = keys.getBinaryValues();
        int[]    keyOffsets    = keys.getBinaryOffsets();
        int[]    valueInts     = values.getInts();
        Validity valueValidity = values.getLeafValidity();
        boolean  mapHasNulls   = mapValidity.hasNulls();
        boolean  valueHasNulls = valueValidity.hasNulls();

        for (int r = 0; r < recordCount; r++) {
            if (mapHasNulls && mapValidity.isNull(r)) continue;  // null map
            int start = entryOffsets[r];
            int end   = entryOffsets[r + 1];
            for (int i = start; i < end; i++) {
                int keyStart = keyOffsets[i];
                int keyLen   = keyOffsets[i + 1] - keyStart;
                String key   = new String(keyBytes, keyStart, keyLen, StandardCharsets.UTF_8);

                if (valueHasNulls && valueValidity.isNull(i)) {
                    // key present, value null
                } else {
                    process(key, valueInts[i]);
                }
            }
        }
    }
}
```

Two orthogonal offset axes show up here, as in `list<string>`: `entryOffsets` walks map entries within a record (across the `getValueCount()` axis), `keyOffsets` walks byte spans within a single key (across the byte axis of `getBinaryValues()`).

If the map sits under an `OPTIONAL` group — e.g. `optional group meta { map<string, int> tags }` — the chain gains a `STRUCT` layer on top. The same key/value lockstep walk applies, with `getLayerValidity(0)` for `meta`, `getLayerValidity(1)` plus `getLayerOffsets(1)` for the map, and `getLeafValidity()` for the value:

```java
Validity metaValidity  = reader.getLayerValidity(0);   // STRUCT layer for `meta`
Validity mapValidity   = reader.getLayerValidity(1);   // REPEATED layer for the map
int[]    entryOffsets  = reader.getLayerOffsets(1);
```

#### Real items only

The leaf array and `getLayerOffsets` carry **real items only** — phantom slots from null/empty parents at any `REPEATED` layer are excluded. `getValueCount()` returns the real leaf count. `STRUCT` layers do not expand or contract the item stream — they carry a validity bitmap of the same length as their parent. Only `REPEATED` layers add cardinality, via their offsets.

#### Counts at each layer

Every per-layer buffer is sized to `count(k)`, defined recursively:

- `count(0) == getRecordCount()`
- For `k > 0`: `count(k)` equals `count(k-1)` if layer `k-1` is `STRUCT`, or `getLayerOffsets(k-1)[count(k-1)]` (the trailing sentinel) if layer `k-1` is `REPEATED`.

The leaf array itself follows the same rule one step past the innermost layer, so `getValueCount()` matches `count(layerCount)`.

Deeper nestings extend the same chain: at depth N you walk `getLayerOffsets(0)` through `getLayerOffsets(N - 1)`, checking `getLayerValidity(k)` (and, for `REPEATED` layers, the zero-length offsets diff that flags an empty container) at each step before descending.

## Reading Multiple Files

When processing multiple Parquet files, use the `Hardwood` class to share a thread pool across readers.
`Hardwood.openAll(List<InputFile>)` returns a `ParquetFileReader` over many files. The same `RowReader`, `ColumnReader`, and `ColumnReaders` APIs apply.

```java
import dev.hardwood.Hardwood;
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

List<InputFile> files = InputFile.ofPaths(
    Path.of("data_2024_01.parquet"),
    Path.of("data_2024_02.parquet"),
    Path.of("data_2024_03.parquet")
);

try (Hardwood hardwood = Hardwood.create();
     ParquetFileReader parquet = hardwood.openAll(files);
     RowReader reader = parquet.rowReader()) {

    while (reader.hasNext()) {
        reader.next();
        // Access data using the same API as a single-file RowReader
        long id = reader.getLong("id");
        String name = reader.getString("name");
    }
}
```

Cross-file prefetching is automatic: when pages from file N are running low, pages from file N+1 are already being prefetched. This eliminates I/O stalls at file boundaries.

The schema of the first file is the reference schema. Each subsequent file is validated against it as it is opened: every projected column must exist with a matching physical type, logical type, and repetition type, otherwise a `SchemaIncompatibleException` is thrown. Non-projected columns are not checked, so files may carry additional columns. With no explicit projection, all columns of the first file are projected and therefore required in every subsequent file.

By default, `Hardwood.create()` sizes the thread pool to the number of available processors. For custom thread pool sizing, use `HardwoodContext` directly:

```java
import dev.hardwood.HardwoodContext;

try (HardwoodContext context = HardwoodContext.create(4);  // 4 threads
     ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), context);
     RowReader rowReader = reader.rowReader()) {
    // ...
}
```

## Accessing File Metadata

Inspecting metadata before reading is useful for understanding file structure, choosing which columns to project, validating files in a pipeline, or building tooling. Hardwood exposes the full Parquet metadata hierarchy without reading any row data.

A Parquet file is organized as follows:

- **FileMetaData** — top-level: row count, schema, key-value metadata (e.g. Spark schema, pandas metadata), and the writer that produced the file (`createdBy`)
- **RowGroup** — a horizontal partition of the data; each row group contains all columns for a subset of rows
- **ColumnChunk** — one column within a row group; holds compression codec, byte sizes, and optional statistics (min/max values, null count) used for predicate pushdown

```java
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import java.util.Map;

try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
    FileMetaData metadata = reader.getFileMetaData();

    System.out.println("Version: " + metadata.version());
    System.out.println("Total rows: " + metadata.numRows());
    System.out.println("Created by: " + metadata.createdBy());

    // Access application-defined key-value metadata (e.g. Spark schema, pandas metadata, Avro schema)
    Map<String, String> kvMetadata = metadata.keyValueMetadata();
    for (Map.Entry<String, String> entry : kvMetadata.entrySet()) {
        System.out.println("  " + entry.getKey() + " = " + entry.getValue());
    }

    // Schema inspection
    FileSchema schema = reader.getFileSchema();
    for (int i = 0; i < schema.getColumnCount(); i++) {
        ColumnSchema column = schema.getColumn(i);
        System.out.println("Column " + i + ": " + column.name()
            + " (" + column.type() + ", " + column.repetitionType()
            + (column.logicalType() != null ? ", " + column.logicalType() : "")
            + ")");
    }

    // Row group and column chunk details
    for (int rg = 0; rg < metadata.rowGroups().size(); rg++) {
        RowGroup rowGroup = metadata.rowGroups().get(rg);
        System.out.println("Row group " + rg + ": "
            + rowGroup.numRows() + " rows, "
            + rowGroup.totalByteSize() + " bytes");

        for (ColumnChunk chunk : rowGroup.columns()) {
            ColumnMetaData col = chunk.metaData();
            System.out.println("  " + col.pathInSchema()
                + " [" + col.codec() + "]"
                + " compressed=" + col.totalCompressedSize()
                + " uncompressed=" + col.totalUncompressedSize());

            // Column statistics (if available)
            Statistics stats = col.statistics();
            if (stats != null && stats.nullCount() != null) {
                System.out.println("    nulls: " + stats.nullCount());
            }
        }
    }
}
```

## Variant Columns

A Parquet column annotated with the `VARIANT` logical type carries semi-structured, JSON-like data in a self-describing binary encoding. Physically it is a group of two required `BYTE_ARRAY` children, `metadata` and `value`, whose bytes together define a Variant value with its own type tag (object, array, string, int, etc.). `getVariant` reads both children and surfaces them through the [`PqVariant`](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md) API.

```java
try (RowReader rowReader = fileReader.rowReader()) {
    while (rowReader.hasNext()) {
        rowReader.next();
        PqVariant v = rowReader.getVariant("event");
        if (v == null) {
            continue;   // SQL NULL
        }

        // Type introspection
        VariantType tag = v.type();         // OBJECT, ARRAY, STRING, INT32, ...
        if (tag == VariantType.OBJECT) {
            PqVariantObject obj = v.asObject();
            String userId  = obj.getString("user_id");
            int    age     = obj.getInt("age");
            Instant ts     = obj.getTimestamp("ts");

            // Nested Variant OBJECT / ARRAY — same vocabulary all the way down
            PqVariantObject addr = obj.getObject("address");
            PqVariantArray  tags = obj.getArray("tags");
        }

        // Raw canonical bytes (for round-tripping or hashing)
        byte[] metadata = v.metadata();
        byte[] value    = v.value();
    }
}
```

The `PqVariantObject` view exposes the same primitive getters as a Parquet struct (`getInt`, `getString`, `getTimestamp`, …), but its complex navigation uses `getObject` and `getArray` (Variant-spec terminology) rather than `getStruct` / `getList` / `getMap`. A `PqVariantArray` is iterable and indexed; elements are heterogeneous `PqVariant`s — inspect each element's `type()` and unwrap appropriately.

**Primitive extraction on `PqVariant`:** When you already hold a `PqVariant` (e.g. an array element) use the `as*()` methods — `asInt`, `asString`, `asTimestamp`, and so on. Each throws `VariantTypeException` if the variant's type tag doesn't match.

**Shredded Variants:** Some writers store part of the payload in a typed sibling column (`typed_value`) alongside `value` for better compression and pushdown. Reassembly is transparent: `metadata()` and `value()` return canonical bytes regardless of whether the file was shredded, so `PqVariant` consumers see a single consistent representation.

**Current limitations**

- **No Variant-aware predicate pushdown.** Filter predicates against a Variant sub-path (e.g. `WHERE v.age > 30`) aren't yet understood by the pushdown pipeline. Filtering still works against the file's physical shredded columns if you know the layout — a `FilterPredicate.gt("v.typed_value.age", 30)` gets row-group and page skipping via ordinary column statistics — but that ties query code to the writer's shredding strategy and misses any rows where the payload sits in the opaque `value` blob instead. Tracked as [#309](https://github.com/hardwood-hq/hardwood/issues/309).
- **No path projection optimization.** Reading only `v.age` from a Variant column still reassembles the whole Variant for each row rather than reading just the shredded `typed_value.age` column. Requires the same variant-aware planning as #309; no separate issue filed yet.

## Geospatial Support

Hardwood reads the Parquet geospatial metadata layer (GEOMETRY / GEOGRAPHY logical types and per-chunk / per-page `GeospatialStatistics`) and offers a bounding-box filter predicate that pushes spatial selectivity down to the row group and page level. Hardwood does not decode WKB payloads itself — geometry decoding is left to the caller, so the reader has no runtime geometry-library dependency. The de-facto standard Java library for this is the [JTS Topology Suite](https://locationtech.github.io/jts/); the snippets below assume JTS, but any WKB decoder works.

To use JTS in the examples below, add the `jts-core` dependency:

```xml
<dependency>
    <groupId>org.locationtech.jts</groupId>
    <artifactId>jts-core</artifactId>
    <version>1.20.0</version>
</dependency>
```

### Identifying Geospatial Columns

GEOMETRY (planar) and GEOGRAPHY (geodesic on an ellipsoid) appear as `LogicalType.GeometryType` and `LogicalType.GeographyType` on the column schema. Both carry a CRS (defaulting to `OGC:CRS84`); GEOGRAPHY also carries an edge-interpolation algorithm.

```java
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
    FileSchema schema = reader.getFileSchema();
    for (int i = 0; i < schema.getColumnCount(); i++) {
        ColumnSchema column = schema.getColumn(i);
        if (column.logicalType() instanceof LogicalType.GeometryType geomType) {
            System.out.println(column.name() + " is a GEOMETRY column, CRS: " + geomType.crs());
        }
        else if (column.logicalType() instanceof LogicalType.GeographyType geoType) {
            System.out.println(column.name() + " is a GEOGRAPHY column, CRS: " + geoType.crs()
                + ", edge interpolation: " + geoType.edgeInterpolation());
        }
    }
}
```

### Bounding-Box Statistics

Per-chunk geospatial statistics are exposed on `ColumnMetaData.geospatialStatistics()`. The `BoundingBox` carries `xmin/xmax/ymin/ymax` (always present) plus optional `zmin/zmax/mmin/mmax`. For GEOGRAPHY columns, `xmin > xmax` is legal and indicates a chunk that wraps the antimeridian. The same struct also appears per-page on `ColumnIndex.geospatialStatistics()` when the file was written with a Page Index.

```java
import dev.hardwood.metadata.BoundingBox;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.GeospatialStatistics;
import dev.hardwood.metadata.RowGroup;

for (RowGroup rowGroup : reader.getFileMetaData().rowGroups()) {
    for (ColumnChunk chunk : rowGroup.columns()) {
        GeospatialStatistics geo = chunk.metaData().geospatialStatistics();
        if (geo == null) {
            continue;
        }
        BoundingBox bbox = geo.bbox();
        if (bbox != null) {
            System.out.println("  bbox: x=[" + bbox.xmin() + ", " + bbox.xmax() + "], "
                + "y=[" + bbox.ymin() + ", " + bbox.ymax() + "]");
        }
        // Geospatial type codes (Point=1, LineString=2, Polygon=3, MultiPoint=4, etc.)
        System.out.println("  types: " + geo.geospatialTypes());
    }
}
```

### Spatial Filter Pushdown

`FilterPredicate.intersects(column, xmin, ymin, xmax, ymax)` produces a predicate that drops row groups and pages whose stored bounding box does not overlap the query box. The argument order follows the GeoJSON / WKT convention (bottom-left corner, then top-right corner). Antimeridian wrapping is handled automatically.

```java
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;

FilterPredicate filter = FilterPredicate.intersects("location", -25.0, 35.0, 45.0, 72.0);
try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(path));
        RowReader rowReader = fileReader.createRowReader(filter)) {
    WKBReader wkbReader = new WKBReader();
    while (rowReader.hasNext()) {
        rowReader.next();
        byte[] wkb = rowReader.getBinary("location");
        // Decode the WKB payload with a geometry library of your choice (e.g. JTS).
        Geometry geom = wkbReader.read(wkb);
        // Apply any exact-geometry tests here.
    }
}
```

The predicate composes with the standard `and` / `or` combinators, e.g.:

```java
FilterPredicate.and(
    FilterPredicate.intersects("location", -5.0, 50.0, 2.0, 60.0),
    FilterPredicate.gt("population", 100_000));
```

### Limitations

`intersects` is **coarse-grained**: Hardwood drops row groups and pages whose bounding box is disjoint from the query box, but every row in a surviving page is returned. Rows whose individual geometry falls outside the query box are emitted along with truly intersecting ones. Apply your own per-row check on the WKB payload (e.g. via JTS) when you need exact geometric filtering — the bounding-box pushdown still saves the I/O for non-overlapping chunks.

Negation of `intersects` is **not supported**: `FilterPredicate.not(FilterPredicate.intersects(...))` throws `UnsupportedOperationException` at resolve time. The chunk-level criterion for "no row intersects" requires bbox containment rather than overlap, and the per-row dual would require decoding every WKB payload inside the reader — which would pull a geometry library into the runtime. If you need "geometries outside this box", read without a spatial filter and apply the negation yourself against `getBinary("location")`.

Both limitations are tracked by [hardwood#414](https://github.com/hardwood-hq/hardwood/issues/414), which proposes opt-in row-level evaluation via an optional WKB-decoder dependency.

## Error Handling

Hardwood throws specific exceptions for common error conditions:

| Exception | When |
|-----------|------|
| `IOException` | Any I/O error: invalid Parquet file (bad magic number, corrupt footer), local-disk read errors, S3 transport failures (after retry exhaustion — see [S3](s3.md)) |
| `UnsupportedOperationException` | Compression codec library not on classpath — the message names the required dependency |
| `IllegalArgumentException` | Accessing a column not in the projection, type mismatch on accessor, or invalid column name |
| `NullPointerException` | Calling a primitive accessor (`getInt`, `getLong`, etc.) on a null field without checking `isNull()` first |
| `NoSuchElementException` | Calling `next()` on a `RowReader` when `hasNext()` returns `false` |
| `IllegalStateException` | Calling `ColumnReader` accessors before `nextBatch()`, or calling nested-column methods on a flat column |

