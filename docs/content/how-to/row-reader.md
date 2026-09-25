<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Row-Oriented Reading

The `RowReader` reads a Parquet file row by row, with typed accessor methods for each field.

!!! example "Try it yourself"
    Want to run it or explore the capabilities yourself? [**Nested Data**](https://github.com/hardwood-hq/hardwood-examples/tree/main/nested-data) walks structs, lists, and maps with the Row API, and [**Hello Hardwood**](https://github.com/hardwood-hq/hardwood-examples/tree/main/hello-hardwood) covers the basics.

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
                for (PqList row : matrix.lists()) {
                    PqIntList innerList = row.ints();
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
    present-but-null value; call `containsKey(key)` to disambiguate.
    Lookup is supported by `String` / `int` / `long` / `byte[]` keys;
    long-tail key types (DATE / TIMESTAMP / DECIMAL / UUID) are reachable
    through `getEntries()` + `Entry.getKey()`. When a key appears more than
    once, the lookup methods follow the Parquet spec's last-value-wins rule
    and surface the value of the last matching entry.

### Typed Accessor Methods

Every accessor takes either a column name (`getInt("column_name")`) or a column index (`getInt(columnIndex)`). The accessor for each Parquet type and the null- and type-mismatch contracts are listed in [Typed Accessors](../reference/accessors.md). Primitive accessors (`getInt`, `getLong`, `getFloat`, `getDouble`, `getBoolean`) throw `NullPointerException` on a null field, so check `isNull()` first.

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

#### Reading the physical value

When you want the raw physical value rather than the decoded logical-type representation (e.g. the INT64 micros backing a `TIMESTAMP`, the INT32 days backing a `DATE`, or the unscaled INT32 / INT64 / `byte[]` backing a `DECIMAL`), call the **typed primitive accessor that matches the column's physical type**:

```java
// TIMESTAMP column backed by INT64 micros
long micros = rowReader.getLong("created_at");

// DATE column backed by INT32 days since epoch
int daysSinceEpoch = rowReader.getInt("birth_date");

// DECIMAL(precision, scale) column backed by INT64
long unscaled = rowReader.getLong("amount");
```

The columns each physical accessor reads are listed under [Physical accessors](../reference/accessors.md#physical-accessors).

#### Decoded generic access

When the column type is not known ahead of time, `getValue` returns the value decoded to its logical-type representation and `getRawValue` the boxed physical value; see [Generic accessors](../reference/accessors.md#generic-accessors). In hot loops, prefer the typed accessors.
