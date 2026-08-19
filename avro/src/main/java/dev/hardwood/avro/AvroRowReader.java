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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import dev.hardwood.avro.internal.AvroPlanNode;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.reader.FileAwareRowReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.StructAccessor;

/// Reads Parquet rows as Avro [GenericRecord] instances.
///
/// Wraps a Hardwood [RowReader] and materializes each row into a
/// `GenericRecord` using the converted Avro schema. Values are stored
/// in Avro's raw representation (e.g. timestamps as `Long`, `bytes`-backed
/// decimals as `ByteBuffer`, `fixed`-typed columns as `GenericData.Fixed`),
/// matching the behavior of parquet-java's `AvroReadSupport`.
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

    private sealed interface ValueLocation
            permits RootFieldLocation, StructFieldLocation, ListElementLocation, MapValueLocation {
        String description();
    }

    private record RootFieldLocation(String name) implements ValueLocation {
        @Override
        public String description() {
            return "field '" + name + "'";
        }
    }

    private record StructFieldLocation(String name) implements ValueLocation {
        @Override
        public String description() {
            return "struct field '" + name + "'";
        }
    }

    private record ListElementLocation(int index) implements ValueLocation {
        @Override
        public String description() {
            return "list element " + index;
        }
    }

    private record MapValueLocation(String key) implements ValueLocation {
        @Override
        public String description() {
            return "map value for key '" + key + "'";
        }
    }

    private enum RecordPosition {
        ROOT,
        STRUCT
    }

    private final RowReader rowReader;

    /// Per-value accessor decisions, taken from the Parquet schema when the Avro
    /// schema was converted. Materialization switches on these rather than on the
    /// converted Avro shape, which no longer distinguishes a Variant group from an
    /// ordinary `{metadata, value}` struct.
    private final AvroPlanNode plan;

    AvroRowReader(RowReader rowReader, AvroPlanNode plan) {
        this.rowReader = rowReader;
        this.plan = plan;
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
        return materializeRecord(rowReader, plan, RecordPosition.ROOT);
    }

    /// Returns the Avro schema used for materialization.
    ///
    /// @return the Avro record schema
    public Schema getSchema() {
        return plan.avro();
    }

    @Override
    public void close() {
        rowReader.close();
    }

    /// Materialize a row or a nested struct. [RowReader] and [PqStruct] are both
    /// [StructAccessor], so the two differ only in the plan node they are read with.
    private GenericRecord materializeRecord(StructAccessor accessor, AvroPlanNode node,
            RecordPosition position) {
        Schema recordSchema = node.avro();
        GenericRecord record = new GenericData.Record(recordSchema);
        int fieldCount = recordSchema.getFields().size();
        for (int i = 0; i < fieldCount; i++) {
            String parquetName = node.child(i).source().name();
            if (accessor.isNull(parquetName)) {
                record.put(i, null);
                continue;
            }
            ValueLocation location = position == RecordPosition.ROOT
                    ? new RootFieldLocation(parquetName)
                    : new StructFieldLocation(parquetName);
            record.put(i, materializeField(accessor, parquetName, node.child(i), location));
        }
        return record;
    }

    private Object materializeField(StructAccessor accessor, String name, AvroPlanNode node,
            ValueLocation location) {
        return switch (node.kind()) {
            case BOOLEAN, INT, LONG, FLOAT, DOUBLE, UNSIGNED_INT32, BINARY, FIXED ->
                    rawToAvro(accessor.getRawValue(name), node, location);
            case STRING -> requireValue(accessor.getString(name), node, location);
            case UUID -> uuidString(requireValue(accessor.getUuid(name), node, location));
            case DECIMAL -> decimalBytes(requireValue(accessor.getDecimal(name), node, location));
            case STRUCT -> materializeRecord(requireValue(accessor.getStruct(name), node, location),
                    node, RecordPosition.STRUCT);
            case VARIANT -> materializeVariant(requireValue(accessor.getVariant(name), node, location),
                    node.avro());
            case LIST -> materializeList(requireValue(accessor.getList(name), node, location),
                    node.listElement());
            case MAP -> materializeMap(requireValue(accessor.getMap(name), node, location),
                    node.mapValue());
            case NULL -> throw materializationFailure(node, location, accessor.getValue(name),
                    "NULL has no non-null materialization");
        };
    }

    /// Represent a value read in its physical form as Avro expects it.
    ///
    /// These kinds take the same shape at every position — a field, a list element,
    /// a map value — because Avro types them as the physical type and leaves any
    /// logical type in the schema. Reading them through one helper is what keeps the
    /// three positions from drifting apart: a temporal column that materializes as a
    /// number in one place and a `java.time` object in another produces records that
    /// contradict their own schema.
    ///
    /// @param raw the value in its physical representation
    /// @param node the plan node for the value's position
    /// @param location the value's position for a materialization failure
    /// @return the value as its Avro schema requires
    private Object rawToAvro(Object raw, AvroPlanNode node, ValueLocation location) {
        return switch (node.kind()) {
            case UNSIGNED_INT32 -> Integer.toUnsignedLong(
                    requireValueType(raw, Integer.class, node, location));
            case BINARY -> wrapBytes(requireValueType(raw, byte[].class, node, location));
            case FIXED -> wrapFixed(requireValueType(raw, byte[].class, node, location), node, location);
            // Already in their Avro representation as read.
            case BOOLEAN -> requireValueType(raw, Boolean.class, node, location);
            case INT -> requireValueType(raw, Integer.class, node, location);
            case LONG -> requireValueType(raw, Long.class, node, location);
            case FLOAT -> requireValueType(raw, Float.class, node, location);
            case DOUBLE -> requireValueType(raw, Double.class, node, location);
            case STRING, UUID, DECIMAL, STRUCT, VARIANT, LIST, MAP, NULL -> throw new IllegalStateException(
                    "Not a physically represented kind: " + node.kind());
        };
    }

    private static GenericRecord materializeVariant(PqVariant variant, Schema recordSchema) {
        GenericRecord record = new GenericData.Record(recordSchema);
        record.put(0, ByteBuffer.wrap(variant.metadata()));
        record.put(1, ByteBuffer.wrap(variant.value()));
        return record;
    }

    private List<Object> materializeList(PqList pqList, AvroPlanNode element) {
        List<Object> result = new ArrayList<>(pqList.size());
        for (int i = 0; i < pqList.size(); i++) {
            if (pqList.isNull(i)) {
                result.add(null);
                continue;
            }
            result.add(materializeListElement(pqList, i, element));
        }
        return result;
    }

    /// Materialize one list element under its plan node.
    ///
    /// [PqList#getRaw] serves the physical value and [PqList#get] the decoded one, so
    /// each kind takes whichever its Avro representation is defined in terms of — the
    /// same split the field and map paths make between their raw and typed accessors.
    ///
    /// Each accessor result is validated against the planned representation. A mismatch
    /// throws an `IllegalArgumentException` naming the element index, Avro type, and
    /// actual Java value type.
    private Object materializeListElement(PqList pqList, int index, AvroPlanNode node) {
        ValueLocation location = new ListElementLocation(index);
        return switch (node.kind()) {
            case BOOLEAN, INT, LONG, FLOAT, DOUBLE, UNSIGNED_INT32, BINARY, FIXED ->
                    rawToAvro(pqList.getRaw(index), node, location);
            // Decoded value: the Avro representation is defined in terms of the logical
            // value, which the raw physical bytes or number do not carry.
            case UUID -> uuidString(requireValueType(pqList.get(index), UUID.class, node, location));
            case DECIMAL -> decimalBytes(requireValueType(pqList.get(index), BigDecimal.class, node, location));
            case STRING -> requireValueType(pqList.get(index), String.class, node, location);
            case STRUCT -> materializeRecord(requireValueType(pqList.get(index), PqStruct.class, node, location),
                    node, RecordPosition.STRUCT);
            case VARIANT -> materializeVariant(requireValueType(pqList.get(index), PqVariant.class, node, location),
                    node.avro());
            case LIST -> materializeList(requireValueType(pqList.get(index), PqList.class, node, location),
                    node.listElement());
            case MAP -> materializeMap(requireValueType(pqList.get(index), PqMap.class, node, location),
                    node.mapValue());
            case NULL -> throw materializationFailure(node, location, pqList.get(index),
                    "NULL has no non-null materialization");
        };
    }

    private Map<String, Object> materializeMap(PqMap pqMap, AvroPlanNode value) {
        Map<String, Object> result = new HashMap<>(pqMap.size());
        for (PqMap.Entry entry : pqMap.getEntries()) {
            String key = entry.getStringKey();
            if (entry.isValueNull()) {
                result.put(key, null);
                continue;
            }
            result.put(key, materializeMapValue(entry, key, value));
        }
        return result;
    }

    private Object materializeMapValue(PqMap.Entry entry, String key, AvroPlanNode node) {
        ValueLocation location = new MapValueLocation(key);
        return switch (node.kind()) {
            case BOOLEAN, INT, LONG, FLOAT, DOUBLE, UNSIGNED_INT32, BINARY, FIXED ->
                    rawToAvro(entry.getRawValue(), node, location);
            case STRING -> requireValue(entry.getStringValue(), node, location);
            case UUID -> uuidString(requireValue(entry.getUuidValue(), node, location));
            case DECIMAL -> decimalBytes(requireValue(entry.getDecimalValue(), node, location));
            case STRUCT -> materializeRecord(requireValue(entry.getStructValue(), node, location),
                    node, RecordPosition.STRUCT);
            case VARIANT -> materializeVariant(requireValue(entry.getVariantValue(), node, location),
                    node.avro());
            case LIST -> materializeList(requireValue(entry.getListValue(), node, location),
                    node.listElement());
            case MAP -> materializeMap(requireValue(entry.getMapValue(), node, location),
                    node.mapValue());
            case NULL -> throw materializationFailure(node, location, entry.getValue(),
                    "NULL has no non-null materialization");
        };
    }

    /// Encode a decimal as the two's-complement big-endian unscaled bytes Avro's
    /// `decimal` logical type expects on a `BYTES` schema.
    private static ByteBuffer decimalBytes(BigDecimal value) {
        return ByteBuffer.wrap(value.unscaledValue().toByteArray());
    }

    private static String uuidString(UUID value) {
        return value.toString();
    }

    private static ByteBuffer wrapBytes(byte[] bytes) {
        return ByteBuffer.wrap(bytes);
    }

    /// Wrap raw bytes as a [GenericData.Fixed] of the schema's declared size.
    ///
    /// Avro requires a `GenericFixed` for a `fixed`-typed field; a bare
    /// [ByteBuffer] is silently accepted by [GenericRecord#put] but fails when the
    /// record is serialized through a `GenericDatumWriter`. A `FIXED_LEN_BYTE_ARRAY`
    /// column stores exactly `type_length` bytes per value, so a payload whose width
    /// does not match the declared `fixed` size is malformed and rejected.
    private GenericData.Fixed wrapFixed(byte[] bytes, AvroPlanNode node, ValueLocation location) {
        Schema fixedSchema = node.avro();
        int size = fixedSchema.getFixedSize();
        if (bytes.length != size) {
            throw materializationFailure(node, location, bytes,
                    "required fixed width " + size + " but received " + bytes.length);
        }
        return new GenericData.Fixed(fixedSchema, bytes);
    }

    private <T> T requireValue(T value, AvroPlanNode node, ValueLocation location) {
        if (value == null) {
            throw materializationFailure(node, location, null, "required non-null value");
        }
        return value;
    }

    private <T> T requireValueType(Object value, Class<T> expectedClass, AvroPlanNode node,
            ValueLocation location) {
        if (!expectedClass.isInstance(value)) {
            throw materializationFailure(node, location, value,
                    "required " + expectedClass.getTypeName());
        }
        return expectedClass.cast(value);
    }

    private IllegalArgumentException materializationFailure(AvroPlanNode node,
            ValueLocation location, Object value, String detail) {
        String actualType = value == null ? "null" : value.getClass().getTypeName();
        return new IllegalArgumentException(filePrefix() + "Cannot materialize " + location.description()
                + " as " + targetType(node) + ": actual Java value type " + actualType
                + " (" + detail + ")");
    }

    /// Name the Avro type a value is being materialized as, adding the plan's kind
    /// where the two differ. A `UINT_32` column is Avro `LONG` but is read as an
    /// `Integer` and widened, so naming the Avro type alone makes the required Java
    /// type look like a contradiction.
    private static String targetType(AvroPlanNode node) {
        String avroType = node.avro().getType().name();
        String kind = node.kind().name();
        return kind.equals(avroType) ? "Avro " + avroType : "Avro " + avroType + " (" + kind + ")";
    }

    /// The `[fileName] ` prefix the core readers put on their own exceptions, so a
    /// failure in a multi-file read names the file the current row came from. Empty
    /// when the underlying reader cannot name one.
    private String filePrefix() {
        return rowReader instanceof FileAwareRowReader fileAware
                ? ExceptionContext.filePrefix(fileAware.currentFileName())
                : "";
    }
}
