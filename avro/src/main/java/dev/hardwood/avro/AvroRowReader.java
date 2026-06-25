/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.avro;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import dev.hardwood.avro.internal.AvroSchemaConverter;
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
            case LONG -> isUnsignedInt32(resolved)
                    ? Integer.toUnsignedLong(reader.getInt(name))
                    : reader.getLong(name);
            case FLOAT -> reader.getFloat(name);
            case DOUBLE -> reader.getDouble(name);
            case STRING -> isUuid(resolved) ? uuidString(reader.getUuid(name)) : reader.getString(name);
            case BYTES -> isDecimal(resolved)
                    ? decimalBytes(reader.getDecimal(name))
                    : wrapBytes(reader.getBinary(name));
            case FIXED -> wrapBytes(reader.getBinary(name));
            case RECORD -> isVariantShape(resolved)
                    ? materializeVariant(reader.getVariant(name), resolved)
                    : materializeStruct(reader.getStruct(name), resolved);
            case ARRAY -> materializeList(reader.getList(name), resolved.getElementType());
            case MAP -> materializeMap(reader.getMap(name), resolved.getValueType());
            default -> reader.getValue(name);
        };
    }

    /// Detect the two-field `{metadata: bytes, value: bytes}` RECORD shape
    /// emitted by [dev.hardwood.avro.internal.AvroSchemaConverter#convertVariant]
    /// for Variant-annotated columns.
    private static boolean isVariantShape(Schema recordSchema) {
        List<Schema.Field> fields = recordSchema.getFields();
        if (fields.size() != 2) {
            return false;
        }
        Schema.Field first = fields.get(0);
        Schema.Field second = fields.get(1);
        return "metadata".equals(first.name())
                && first.schema().getType() == Schema.Type.BYTES
                && "value".equals(second.name())
                && second.schema().getType() == Schema.Type.BYTES;
    }

    private static GenericRecord materializeVariant(dev.hardwood.row.PqVariant variant, Schema recordSchema) {
        if (variant == null) {
            return null;
        }
        GenericRecord record = new GenericData.Record(recordSchema);
        record.put(0, ByteBuffer.wrap(variant.metadata()));
        record.put(1, ByteBuffer.wrap(variant.value()));
        return record;
    }

    private Object materializeStructValue(PqStruct struct, String name, Schema schema) {
        Schema resolved = resolveUnion(schema);
        return switch (resolved.getType()) {
            case BOOLEAN -> struct.getBoolean(name);
            case INT -> struct.getInt(name);
            case LONG -> isUnsignedInt32(resolved)
                    ? Integer.toUnsignedLong(struct.getInt(name))
                    : struct.getLong(name);
            case FLOAT -> struct.getFloat(name);
            case DOUBLE -> struct.getDouble(name);
            case STRING -> isUuid(resolved) ? uuidString(struct.getUuid(name)) : struct.getString(name);
            case BYTES -> isDecimal(resolved)
                    ? decimalBytes(struct.getDecimal(name))
                    : wrapBytes(struct.getBinary(name));
            case FIXED -> wrapBytes(struct.getBinary(name));
            case RECORD -> isVariantShape(resolved)
                    ? materializeVariant(struct.getVariant(name), resolved)
                    : materializeStruct(struct.getStruct(name), resolved);
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
            case LONG -> {
                Object raw = pqList.get(index);
                yield isUnsignedInt32(elementSchema) && raw instanceof Integer i
                        ? Integer.toUnsignedLong(i)
                        : raw;
            }
            case FLOAT -> pqList.get(index);
            case DOUBLE -> pqList.get(index);
            case STRING -> {
                Object val = pqList.get(index);
                yield isUuid(elementSchema) && val instanceof UUID u ? u.toString() : val;
            }
            case BYTES -> {
                Object val = pqList.get(index);
                if (isDecimal(elementSchema) && val instanceof BigDecimal d) {
                    yield decimalBytes(d);
                }
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
            case LONG -> isUnsignedInt32(valueSchema)
                    ? Integer.toUnsignedLong(entry.getIntValue())
                    : entry.getLongValue();
            case FLOAT -> entry.getFloatValue();
            case DOUBLE -> entry.getDoubleValue();
            case STRING -> isUuid(valueSchema) ? uuidString(entry.getUuidValue()) : entry.getStringValue();
            case BYTES -> isDecimal(valueSchema)
                    ? decimalBytes(entry.getDecimalValue())
                    : wrapBytes(entry.getBinaryValue());
            case RECORD -> materializeStruct(entry.getStructValue(), valueSchema);
            case ARRAY -> materializeList(entry.getListValue(), valueSchema.getElementType());
            case MAP -> materializeMap(entry.getMapValue(), valueSchema.getValueType());
            default -> entry.getValue();
        };
    }

    private static boolean isUnsignedInt32(Schema schema) {
        return Boolean.TRUE.equals(schema.getObjectProp(AvroSchemaConverter.UNSIGNED_INT32_PROP));
    }

    /// True for an Avro `BYTES` schema carrying the decimal logical type. Such a
    /// column may be physically `INT32`/`INT64`/`BYTE_ARRAY`-backed, so it must be
    /// read through the logical `getDecimal` accessor rather than `getBinary`.
    private static boolean isDecimal(Schema schema) {
        return schema.getLogicalType() instanceof LogicalTypes.Decimal;
    }

    /// True for an Avro `STRING` schema carrying the uuid logical type. The source
    /// column is `FIXED_LEN_BYTE_ARRAY`, so it must be read through `getUuid` and
    /// rendered as the canonical UUID string rather than decoded as raw bytes.
    private static boolean isUuid(Schema schema) {
        LogicalType logicalType = schema.getLogicalType();
        return logicalType != null && "uuid".equals(logicalType.getName());
    }

    /// Encode a decimal as the two's-complement big-endian unscaled bytes Avro's
    /// `decimal` logical type expects on a `BYTES` schema.
    private static ByteBuffer decimalBytes(BigDecimal value) {
        return value == null ? null : ByteBuffer.wrap(value.unscaledValue().toByteArray());
    }

    private static String uuidString(UUID value) {
        return value == null ? null : value.toString();
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
