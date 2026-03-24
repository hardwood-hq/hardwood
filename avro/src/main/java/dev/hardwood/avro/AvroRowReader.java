/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;

/// Reads Parquet rows as Avro [GenericRecord] instances.
///
/// Wraps a Hardwood [RowReader] and materializes each row into a
/// `GenericRecord` using the converted Avro schema. Values are stored
/// in Avro's raw representation (e.g. timestamps as `Long`, decimals
/// as `ByteBuffer`), matching the behavior of parquet-java's
/// `AvroReadSupport`.
///
/// ```java
/// try (AvroRowReader reader = AvroReaders.createRowReader(fileReader)) {
///     while (reader.hasNext()) {
///         GenericRecord record = reader.next();
///         long id = (Long) record.get("id");
///     }
/// }
/// ```
public class AvroRowReader implements AutoCloseable {

    private final RowReader rowReader;
    private final Schema avroSchema;

    AvroRowReader(RowReader rowReader, Schema avroSchema) {
        this.rowReader = rowReader;
        this.avroSchema = avroSchema;
    }

    /// Check if there are more rows to read.
    ///
    /// @return true if there are more rows
    public boolean hasNext() {
        return rowReader.hasNext();
    }

    /// Advance to the next row and return it as a GenericRecord.
    ///
    /// @return the current row as a GenericRecord
    public GenericRecord next() {
        rowReader.next();
        return materializeRow();
    }

    /// Returns the Avro schema used for materialization.
    ///
    /// @return the Avro record schema
    public Schema getSchema() {
        return avroSchema;
    }

    @Override
    public void close() {
        rowReader.close();
    }

    private GenericRecord materializeRow() {
        GenericRecord record = new GenericData.Record(avroSchema);
        for (Schema.Field field : avroSchema.getFields()) {
            String name = field.name();
            if (rowReader.isNull(name)) {
                record.put(field.pos(), null);
                continue;
            }
            record.put(field.pos(), materializeValue(rowReader, name, field.schema()));
        }
        return record;
    }

    private Object materializeValue(RowReader reader, String name, Schema schema) {
        Schema resolved = resolveUnion(schema);
        return switch (resolved.getType()) {
            case BOOLEAN -> reader.getBoolean(name);
            case INT -> reader.getInt(name);
            case LONG -> reader.getLong(name);
            case FLOAT -> reader.getFloat(name);
            case DOUBLE -> reader.getDouble(name);
            case STRING -> reader.getString(name);
            case BYTES -> wrapBytes(reader.getBinary(name));
            case FIXED -> wrapBytes(reader.getBinary(name));
            case RECORD -> materializeStruct(reader.getStruct(name), resolved);
            case ARRAY -> materializeList(reader.getList(name), resolved.getElementType());
            case MAP -> materializeMap(reader.getMap(name), resolved.getValueType());
            default -> reader.getValue(name);
        };
    }

    private Object materializeStructValue(PqStruct struct, String name, Schema schema) {
        Schema resolved = resolveUnion(schema);
        return switch (resolved.getType()) {
            case BOOLEAN -> struct.getBoolean(name);
            case INT -> struct.getInt(name);
            case LONG -> struct.getLong(name);
            case FLOAT -> struct.getFloat(name);
            case DOUBLE -> struct.getDouble(name);
            case STRING -> struct.getString(name);
            case BYTES -> wrapBytes(struct.getBinary(name));
            case FIXED -> wrapBytes(struct.getBinary(name));
            case RECORD -> materializeStruct(struct.getStruct(name), resolved);
            case ARRAY -> materializeList(struct.getList(name), resolved.getElementType());
            case MAP -> materializeMap(struct.getMap(name), resolved.getValueType());
            default -> struct.getValue(name);
        };
    }

    private GenericRecord materializeStruct(PqStruct struct, Schema recordSchema) {
        GenericRecord record = new GenericData.Record(recordSchema);
        for (Schema.Field field : recordSchema.getFields()) {
            String name = field.name();
            if (struct.isNull(name)) {
                record.put(field.pos(), null);
                continue;
            }
            record.put(field.pos(), materializeStructValue(struct, name, field.schema()));
        }
        return record;
    }

    private List<Object> materializeList(PqList pqList, Schema elementSchema) {
        Schema resolved = resolveUnion(elementSchema);
        List<Object> result = new ArrayList<>(pqList.size());
        for (int i = 0; i < pqList.size(); i++) {
            if (pqList.isNull(i)) {
                result.add(null);
                continue;
            }
            result.add(materializeListElement(pqList, i, resolved));
        }
        return result;
    }

    private Object materializeListElement(PqList pqList, int index, Schema elementSchema) {
        return switch (elementSchema.getType()) {
            case BOOLEAN -> pqList.get(index);
            case INT -> pqList.get(index);
            case LONG -> pqList.get(index);
            case FLOAT -> pqList.get(index);
            case DOUBLE -> pqList.get(index);
            case STRING -> pqList.get(index);
            case BYTES -> {
                Object val = pqList.get(index);
                yield val instanceof byte[] bytes ? ByteBuffer.wrap(bytes) : val;
            }
            case RECORD -> {
                Object val = pqList.get(index);
                yield val instanceof PqStruct struct ? materializeStruct(struct, elementSchema) : val;
            }
            case ARRAY -> {
                Object val = pqList.get(index);
                yield val instanceof PqList nested
                        ? materializeList(nested, elementSchema.getElementType())
                        : val;
            }
            case MAP -> {
                Object val = pqList.get(index);
                yield val instanceof PqMap nested
                        ? materializeMap(nested, elementSchema.getValueType())
                        : val;
            }
            default -> pqList.get(index);
        };
    }

    private Map<String, Object> materializeMap(PqMap pqMap, Schema valueSchema) {
        Schema resolved = resolveUnion(valueSchema);
        Map<String, Object> result = new HashMap<>(pqMap.size());
        for (PqMap.Entry entry : pqMap.getEntries()) {
            String key = entry.getStringKey();
            if (entry.isValueNull()) {
                result.put(key, null);
                continue;
            }
            result.put(key, materializeMapValue(entry, resolved));
        }
        return result;
    }

    private Object materializeMapValue(PqMap.Entry entry, Schema valueSchema) {
        return switch (valueSchema.getType()) {
            case BOOLEAN -> entry.getBooleanValue();
            case INT -> entry.getIntValue();
            case LONG -> entry.getLongValue();
            case FLOAT -> entry.getFloatValue();
            case DOUBLE -> entry.getDoubleValue();
            case STRING -> entry.getStringValue();
            case BYTES -> wrapBytes(entry.getBinaryValue());
            case RECORD -> materializeStruct(entry.getStructValue(), valueSchema);
            case ARRAY -> materializeList(entry.getListValue(), valueSchema.getElementType());
            case MAP -> materializeMap(entry.getMapValue(), valueSchema.getValueType());
            default -> entry.getValue();
        };
    }

    private static Schema resolveUnion(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema member : schema.getTypes()) {
                if (member.getType() != Schema.Type.NULL) {
                    return member;
                }
            }
        }
        return schema;
    }

    private static ByteBuffer wrapBytes(byte[] bytes) {
        return bytes != null ? ByteBuffer.wrap(bytes) : null;
    }
}
