/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.example.data.simple;

import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqStruct;

/**
 * SimpleGroup implementation that wraps Hardwood's RowReader.
 * <p>
 * This class provides parquet-java compatible Group access by capturing
 * a snapshot of the row data from Hardwood's RowReader API.
 * </p>
 */
public class SimpleGroup implements Group {

    private final Map<String, Object> values;
    private final Map<String, Boolean> nullFlags;
    private final GroupType schema;

    /**
     * Create a SimpleGroup by capturing data from a RowReader at its current position.
     *
     * @param rowReader the Hardwood RowReader positioned at the row to capture
     * @param schema the GroupType schema
     */
    public SimpleGroup(RowReader rowReader, GroupType schema) {
        this.schema = schema;
        this.values = new HashMap<>();
        this.nullFlags = new HashMap<>();
        captureRowData(rowReader);
    }

    /**
     * Create a SimpleGroup wrapping a nested PqStruct.
     *
     * @param struct the Hardwood PqStruct
     * @param schema the GroupType schema
     */
    public SimpleGroup(PqStruct struct, GroupType schema) {
        this.schema = schema;
        this.values = new HashMap<>();
        this.nullFlags = new HashMap<>();
        captureStructData(struct);
    }

    private void captureRowData(RowReader rowReader) {
        for (int i = 0; i < schema.getFieldCount(); i++) {
            Type fieldType = schema.getType(i);
            String fieldName = fieldType.getName();
            boolean isNull = rowReader.isNull(fieldName);
            nullFlags.put(fieldName, isNull);
            if (!isNull) {
                values.put(fieldName, captureValue(rowReader, fieldName, fieldType));
            }
        }
    }

    private void captureStructData(PqStruct struct) {
        for (int i = 0; i < schema.getFieldCount(); i++) {
            Type fieldType = schema.getType(i);
            String fieldName = fieldType.getName();
            boolean isNull = struct.isNull(fieldName);
            nullFlags.put(fieldName, isNull);
            if (!isNull) {
                values.put(fieldName, captureStructValue(struct, fieldName, fieldType));
            }
        }
    }

    private Object captureValue(RowReader rowReader, String fieldName, Type fieldType) {
        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            return rowReader.getList(fieldName);
        }
        if (fieldType.isPrimitive()) {
            return capturePrimitiveFromRowReader(rowReader, fieldName, fieldType);
        }
        // Group type - check if it's a LIST or MAP
        GroupType groupType = fieldType.asGroupType();
        if (isListType(groupType)) {
            return rowReader.getList(fieldName);
        }
        if (isMapType(groupType)) {
            return rowReader.getMap(fieldName);
        }
        // Nested struct
        return rowReader.getStruct(fieldName);
    }

    private Object captureStructValue(PqStruct struct, String fieldName, Type fieldType) {
        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            return struct.getList(fieldName);
        }
        if (fieldType.isPrimitive()) {
            return capturePrimitiveFromStruct(struct, fieldName, fieldType);
        }
        // Group type - check if it's a LIST or MAP
        GroupType groupType = fieldType.asGroupType();
        if (isListType(groupType)) {
            return struct.getList(fieldName);
        }
        if (isMapType(groupType)) {
            return struct.getMap(fieldName);
        }
        // Nested struct
        return struct.getStruct(fieldName);
    }

    private boolean isListType(GroupType groupType) {
        var originalType = groupType.getOriginalType();
        return originalType == org.apache.parquet.schema.OriginalType.LIST;
    }

    private boolean isMapType(GroupType groupType) {
        var originalType = groupType.getOriginalType();
        return originalType == org.apache.parquet.schema.OriginalType.MAP
                || originalType == org.apache.parquet.schema.OriginalType.MAP_KEY_VALUE;
    }

    private Object capturePrimitiveFromRowReader(RowReader rowReader, String fieldName, Type fieldType) {
        return switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
            case INT32 -> rowReader.getInt(fieldName);
            case INT64 -> rowReader.getLong(fieldName);
            case FLOAT -> rowReader.getFloat(fieldName);
            case DOUBLE -> rowReader.getDouble(fieldName);
            case BOOLEAN -> rowReader.getBoolean(fieldName);
            case BINARY, FIXED_LEN_BYTE_ARRAY -> rowReader.getBinary(fieldName);
            case INT96 -> rowReader.getBinary(fieldName);
        };
    }

    private Object capturePrimitiveFromStruct(PqStruct struct, String fieldName, Type fieldType) {
        return switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
            case INT32 -> struct.getInt(fieldName);
            case INT64 -> struct.getLong(fieldName);
            case FLOAT -> struct.getFloat(fieldName);
            case DOUBLE -> struct.getDouble(fieldName);
            case BOOLEAN -> struct.getBoolean(fieldName);
            case BINARY, FIXED_LEN_BYTE_ARRAY -> struct.getBinary(fieldName);
            case INT96 -> struct.getBinary(fieldName);
        };
    }

    @Override
    public GroupType getType() {
        return schema;
    }

    @Override
    public int getFieldRepetitionCount(int fieldIndex) {
        Type fieldType = schema.getType(fieldIndex);
        String fieldName = fieldType.getName();
        if (Boolean.TRUE.equals(nullFlags.get(fieldName))) {
            return 0;
        }
        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            PqList list = (PqList) values.get(fieldName);
            return list != null ? list.size() : 0;
        }
        return 1;
    }

    @Override
    public int getFieldRepetitionCount(String field) {
        return getFieldRepetitionCount(schema.getFieldIndex(field));
    }

    // ---- String getters ----

    @Override
    public String getString(int fieldIndex, int index) {
        Type fieldType = schema.getType(fieldIndex);
        String fieldName = fieldType.getName();
        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            PqList list = (PqList) values.get(fieldName);
            if (list == null || index >= list.size()) {
                return null;
            }
            return (String) list.get(index);
        }
        else {
            if (index != 0) {
                throw new IndexOutOfBoundsException("Index must be 0 for non-repeated fields, got: " + index);
            }
            Object value = values.get(fieldName);
            if (value instanceof byte[] bytes) {
                return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            }
            return (String) value;
        }
    }

    @Override
    public String getString(String field, int index) {
        return getString(schema.getFieldIndex(field), index);
    }

    // ---- Integer getters ----

    @Override
    public int getInteger(int fieldIndex, int index) {
        Type fieldType = schema.getType(fieldIndex);
        String fieldName = fieldType.getName();
        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            PqList list = (PqList) values.get(fieldName);
            if (list == null || index >= list.size()) {
                return 0;
            }
            Integer value = (Integer) list.get(index);
            return value != null ? value : 0;
        }
        else {
            if (index != 0) {
                throw new IndexOutOfBoundsException("Index must be 0 for non-repeated fields, got: " + index);
            }
            Integer value = (Integer) values.get(fieldName);
            return value != null ? value : 0;
        }
    }

    @Override
    public int getInteger(String field, int index) {
        return getInteger(schema.getFieldIndex(field), index);
    }

    // ---- Long getters ----

    @Override
    public long getLong(int fieldIndex, int index) {
        Type fieldType = schema.getType(fieldIndex);
        String fieldName = fieldType.getName();
        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            PqList list = (PqList) values.get(fieldName);
            if (list == null || index >= list.size()) {
                return 0L;
            }
            Long value = (Long) list.get(index);
            return value != null ? value : 0L;
        }
        else {
            if (index != 0) {
                throw new IndexOutOfBoundsException("Index must be 0 for non-repeated fields, got: " + index);
            }
            Long value = (Long) values.get(fieldName);
            return value != null ? value : 0L;
        }
    }

    @Override
    public long getLong(String field, int index) {
        return getLong(schema.getFieldIndex(field), index);
    }

    // ---- Double getters ----

    @Override
    public double getDouble(int fieldIndex, int index) {
        Type fieldType = schema.getType(fieldIndex);
        String fieldName = fieldType.getName();
        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            PqList list = (PqList) values.get(fieldName);
            if (list == null || index >= list.size()) {
                return 0.0;
            }
            Double value = (Double) list.get(index);
            return value != null ? value : 0.0;
        }
        else {
            if (index != 0) {
                throw new IndexOutOfBoundsException("Index must be 0 for non-repeated fields, got: " + index);
            }
            Double value = (Double) values.get(fieldName);
            return value != null ? value : 0.0;
        }
    }

    @Override
    public double getDouble(String field, int index) {
        return getDouble(schema.getFieldIndex(field), index);
    }

    // ---- Float getters ----

    @Override
    public float getFloat(int fieldIndex, int index) {
        Type fieldType = schema.getType(fieldIndex);
        String fieldName = fieldType.getName();
        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            PqList list = (PqList) values.get(fieldName);
            if (list == null || index >= list.size()) {
                return 0.0f;
            }
            Float value = (Float) list.get(index);
            return value != null ? value : 0.0f;
        }
        else {
            if (index != 0) {
                throw new IndexOutOfBoundsException("Index must be 0 for non-repeated fields, got: " + index);
            }
            Float value = (Float) values.get(fieldName);
            return value != null ? value : 0.0f;
        }
    }

    @Override
    public float getFloat(String field, int index) {
        return getFloat(schema.getFieldIndex(field), index);
    }

    // ---- Boolean getters ----

    @Override
    public boolean getBoolean(int fieldIndex, int index) {
        Type fieldType = schema.getType(fieldIndex);
        String fieldName = fieldType.getName();
        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            PqList list = (PqList) values.get(fieldName);
            if (list == null || index >= list.size()) {
                return false;
            }
            Boolean value = (Boolean) list.get(index);
            return value != null ? value : false;
        }
        else {
            if (index != 0) {
                throw new IndexOutOfBoundsException("Index must be 0 for non-repeated fields, got: " + index);
            }
            Boolean value = (Boolean) values.get(fieldName);
            return value != null ? value : false;
        }
    }

    @Override
    public boolean getBoolean(String field, int index) {
        return getBoolean(schema.getFieldIndex(field), index);
    }

    // ---- Binary getters ----

    @Override
    public Binary getBinary(int fieldIndex, int index) {
        Type fieldType = schema.getType(fieldIndex);
        String fieldName = fieldType.getName();
        byte[] bytes;
        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            PqList list = (PqList) values.get(fieldName);
            if (list == null || index >= list.size()) {
                return null;
            }
            bytes = (byte[]) list.get(index);
        }
        else {
            if (index != 0) {
                throw new IndexOutOfBoundsException("Index must be 0 for non-repeated fields, got: " + index);
            }
            bytes = (byte[]) values.get(fieldName);
        }
        return bytes != null ? Binary.fromConstantByteArray(bytes) : null;
    }

    @Override
    public Binary getBinary(String field, int index) {
        return getBinary(schema.getFieldIndex(field), index);
    }

    // ---- Group getters ----

    @Override
    public Group getGroup(int fieldIndex, int index) {
        Type fieldType = schema.getType(fieldIndex);
        String fieldName = fieldType.getName();
        GroupType nestedType = fieldType.asGroupType();

        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            PqList list = (PqList) values.get(fieldName);
            if (list == null || index >= list.size()) {
                return null;
            }
            PqStruct nestedStruct = (PqStruct) list.get(index);
            return nestedStruct != null ? new SimpleGroup(nestedStruct, nestedType) : null;
        }
        else {
            if (index != 0) {
                throw new IndexOutOfBoundsException("Index must be 0 for non-repeated fields, got: " + index);
            }
            PqStruct nestedStruct = (PqStruct) values.get(fieldName);
            return nestedStruct != null ? new SimpleGroup(nestedStruct, nestedType) : null;
        }
    }

    @Override
    public Group getGroup(String field, int index) {
        return getGroup(schema.getFieldIndex(field), index);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(schema.getName()).append(" {");
        for (int i = 0; i < schema.getFieldCount(); i++) {
            if (i > 0)
                sb.append(", ");
            Type fieldType = schema.getType(i);
            sb.append(fieldType.getName()).append("=");
            int count = getFieldRepetitionCount(i);
            if (count == 0) {
                sb.append("null");
            }
            else if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
                sb.append("[").append(count).append(" values]");
            }
            else {
                sb.append("...");
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
