<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Row-Oriented Reading

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

