# Design: Avro GenericRecord support in Hardwood core

**Status: Implemented**

## Goal

Let users read Parquet files into Avro `GenericRecord` via Hardwood's native reader,
without depending on parquet-java. This is a core Hardwood feature (not a compat shim),
because GenericRecord is a widely-used record representation independent of parquet-java.

Two integration points:
1. **Hardwood core API** — `ParquetFileReader` produces `GenericRecord` directly
2. **Compat layer** — `AvroParquetReader` shim delegates to the core API

This design covers (1). The compat shim (issue #130) becomes trivial once the core API exists.

## User-facing API

```java
// Convert Parquet schema to Avro schema
Schema avroSchema = AvroSchemaConverter.convert(fileReader.getFileSchema());

// Read rows as GenericRecord
try (AvroRowReader reader = fileReader.createAvroRowReader()) {
    while (reader.hasNext()) {
        GenericRecord record = reader.next();
        String name = (String) record.get("name");
        long id = (Long) record.get("id");
    }
}

// With column projection
try (AvroRowReader reader = fileReader.createAvroRowReader(
        ColumnProjection.of("id", "name"))) {
    // ...
}

// With filter
try (AvroRowReader reader = fileReader.createAvroRowReader(filter)) {
    // ...
}
```

## New module: `hardwood-avro`

A separate module rather than adding Avro to core, because:
- `org.apache.avro:avro` is a non-trivial dependency (~1.8 MB, pulls in Jackson)
- Users who don't need GenericRecord shouldn't pay for it
- Follows the same pattern as `hardwood-s3`

### Module structure

```
avro/
  pom.xml                    (depends on hardwood-core + org.apache.avro:avro)
  src/main/java/dev/hardwood/avro/
    AvroSchemaConverter.java  (FileSchema → Avro Schema)
    AvroRowReader.java        (wraps RowReader, produces GenericRecord)
```

### Dependencies

```xml
<dependency>
  <groupId>dev.hardwood</groupId>
  <artifactId>hardwood-core</artifactId>
</dependency>
<dependency>
  <groupId>org.apache.avro</groupId>
  <artifactId>avro</artifactId>
  <version>1.12.0</version>
</dependency>
```

## Component 1: `AvroSchemaConverter`

Converts Hardwood's `FileSchema` (list of `SchemaElement`) to an Avro `Schema`.

### Type mapping

| Parquet Physical | Parquet Logical | Avro Type | Avro Logical | Java value in GenericRecord |
|---|---|---|---|---|
| BOOLEAN | — | BOOLEAN | — | `Boolean` |
| INT32 | — | INT | — | `Integer` |
| INT32 | INT_8, INT_16 | INT | — | `Integer` |
| INT32 | UINT_8, UINT_16 | INT | — | `Integer` |
| INT32 | UINT_32 | LONG | — | `Long` |
| INT32 | DATE | INT | date | `Integer` (days since epoch) |
| INT32 | TIME_MILLIS | INT | time-millis | `Integer` |
| INT32 | DECIMAL | FIXED/BYTES | decimal | `ByteBuffer` |
| INT64 | — | LONG | — | `Long` |
| INT64 | UINT_64 | LONG | — | `Long` |
| INT64 | TIMESTAMP_MILLIS | LONG | timestamp-millis | `Long` |
| INT64 | TIMESTAMP_MICROS | LONG | timestamp-micros | `Long` |
| INT64 | TIME_MICROS | LONG | time-micros | `Long` |
| INT64 | DECIMAL | FIXED/BYTES | decimal | `ByteBuffer` |
| FLOAT | — | FLOAT | — | `Float` |
| DOUBLE | — | DOUBLE | — | `Double` |
| BYTE_ARRAY | — | BYTES | — | `ByteBuffer` |
| BYTE_ARRAY | STRING, ENUM, JSON | STRING | — | `String` |
| BYTE_ARRAY | DECIMAL | BYTES | decimal | `ByteBuffer` |
| FIXED_LEN_BYTE_ARRAY | — | FIXED | — | `GenericData.Fixed` |
| FIXED_LEN_BYTE_ARRAY | DECIMAL | FIXED | decimal | `GenericData.Fixed` |
| FIXED_LEN_BYTE_ARRAY | UUID | STRING | uuid | `String` |
| INT96 | — | FIXED(12) | — | `GenericData.Fixed` |
| Group | — | RECORD | — | `GenericRecord` |
| Group | LIST | ARRAY | — | `List<?>` |
| Group | MAP | MAP | — | `Map<String, ?>` |

### Nullability

Parquet `OPTIONAL` fields become Avro `UNION [NULL, type]`.
Parquet `REQUIRED` fields map directly to the Avro type (no union).
Parquet `REPEATED` fields at top level map to Avro `ARRAY`.

### Nested schemas

The converter walks the `SchemaElement` tree recursively:
- Group elements → Avro RECORD with nested fields
- LIST logical type → unwrap the 3-level encoding, emit Avro ARRAY of element type
- MAP logical type → unwrap key/value groups, emit Avro MAP with value type

### API

```java
public final class AvroSchemaConverter {

    /** Convert a Hardwood FileSchema to an Avro Schema. */
    public static Schema convert(FileSchema fileSchema);

    /** Convert a single SchemaElement subtree to an Avro Schema. */
    static Schema convertElement(SchemaElement element, FileSchema fileSchema);
}
```

## Component 2: `AvroRowReader`

Wraps Hardwood's `RowReader` and materializes each row as a `GenericRecord`.

### Design

```java
public class AvroRowReader implements AutoCloseable {

    private final RowReader rowReader;
    private final Schema avroSchema;
    private final GroupType messageType; // for field index lookups

    AvroRowReader(RowReader rowReader, Schema avroSchema, GroupType messageType);

    public boolean hasNext();

    /** Advance to next row and return it as a GenericRecord. */
    public GenericRecord next();

    public Schema getSchema();

    @Override
    public void close();
}
```

### Materialization strategy

On each `next()` call:
1. Call `rowReader.next()` to advance
2. Create `new GenericData.Record(avroSchema)`
3. For each field in the schema:
   - Check `rowReader.isNull(fieldName)` → put `null` for nullable fields
   - Read the value using the appropriate typed getter (`getInt`, `getLong`, `getBinary`, etc.)
   - Convert to the Avro-expected Java type if needed (e.g., `byte[]` → `ByteBuffer` for BYTES)
   - For nested records: `rowReader.getStruct(fieldName)` → recursively materialize via `PqStruct`
   - For lists: `rowReader.getList(fieldName)` → materialize `PqList` into `java.util.List`
   - For maps: `rowReader.getMap(fieldName)` → materialize `PqMap` into `java.util.Map`
4. Return the populated `GenericRecord`

### Value conversion details

| Hardwood getter | Avro value | Conversion |
|---|---|---|
| `getInt()` | `Integer` | direct (autoboxing) |
| `getLong()` | `Long` | direct |
| `getFloat()` | `Float` | direct |
| `getDouble()` | `Double` | direct |
| `getBoolean()` | `Boolean` | direct |
| `getBinary()` | `ByteBuffer` | `ByteBuffer.wrap(bytes)` |
| `getString()` | `String` | direct |
| `getStruct()` | `GenericRecord` | recursive materialization |
| `getList()` | `List<?>` | iterate PqList, convert elements |
| `getMap()` | `Map<String, ?>` | iterate PqMap entries, convert values |

### Nested record materialization

```java
private GenericRecord materializeStruct(PqStruct struct, Schema recordSchema) {
    GenericRecord record = new GenericData.Record(recordSchema);
    for (Schema.Field field : recordSchema.getFields()) {
        String name = field.name();
        if (struct.isNull(name)) {
            record.put(field.pos(), null);
            continue;
        }
        record.put(field.pos(), materializeValue(struct, name, field.schema()));
    }
    return record;
}
```

## Integration with `ParquetFileReader`

Add convenience methods to `ParquetFileReader`. Since `hardwood-avro` depends on
`hardwood-core` (not the other way around), these methods live in the avro module
as static factories or extension methods:

```java
public final class AvroReaders {

    public static AvroRowReader createAvroRowReader(ParquetFileReader reader);

    public static AvroRowReader createAvroRowReader(
            ParquetFileReader reader, FilterPredicate filter);

    public static AvroRowReader createAvroRowReader(
            ParquetFileReader reader, ColumnProjection projection);

    public static AvroRowReader createAvroRowReader(
            ParquetFileReader reader, ColumnProjection projection,
            FilterPredicate filter);
}
```

## Compat layer integration (issue #130)

Once the core API exists, the compat shim becomes trivial:

```java
// In parquet-java-compat
public class AvroParquetReader {
    public static <T> Builder<T> builder(Path path) { ... }

    // Builder.build() does:
    //   1. Open ParquetFileReader
    //   2. Create AvroRowReader via AvroReaders
    //   3. Wrap in ParquetReader<GenericRecord> adapter
}
```

## Testing

### Test file generation

Extend `tools/simple-datagen.py` to produce Avro-specific test files, or reuse existing
test files (the Parquet format is the same regardless of how data was written).

### Test cases

1. **Flat schema** — all primitive types, read as GenericRecord, verify Java types
2. **Nullable fields** — OPTIONAL → union [null, type], verify null handling
3. **Nested structs** — Group → nested GenericRecord
4. **Lists** — LIST → java.util.List in GenericRecord
5. **Maps** — MAP → java.util.Map in GenericRecord
6. **Logical types** — DATE, TIMESTAMP, DECIMAL → correct Avro logical type annotation
7. **Column projection** — only projected fields populated
8. **Filter pushdown** — combined with GenericRecord output
9. **Schema converter round-trip** — convert FileSchema → Avro Schema, verify field names/types/nullability

## File summary

| File | Module | Action |
|------|--------|--------|
| `avro/pom.xml` | hardwood-avro | New module |
| `avro/src/main/java/dev/hardwood/avro/AvroSchemaConverter.java` | hardwood-avro | New |
| `avro/src/main/java/dev/hardwood/avro/AvroRowReader.java` | hardwood-avro | New |
| `avro/src/main/java/dev/hardwood/avro/AvroReaders.java` | hardwood-avro | New |
| `avro/src/test/java/dev/hardwood/avro/AvroRowReaderTest.java` | hardwood-avro | New |
| `avro/src/test/java/dev/hardwood/avro/AvroSchemaConverterTest.java` | hardwood-avro | New |
| `bom/pom.xml` | hardwood-bom | Add hardwood-avro |
| `pom.xml` | parent | Add avro module |
| `parquet-java-compat/pom.xml` | parquet-java-compat | Optional dep on hardwood-avro |
