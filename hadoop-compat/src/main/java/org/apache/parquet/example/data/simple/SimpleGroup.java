/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.example.data.simple;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqRow;

/**
 * SimpleGroup implementation that wraps Hardwood's PqRow.
 * <p>
 * This class provides parquet-java compatible Group access by delegating
 * to Hardwood's PqRow API.
 * </p>
 */
public class SimpleGroup implements Group {

    private final PqRow row;
    private final GroupType schema;

    /**
     * Create a SimpleGroup wrapping a PqRow.
     *
     * @param row the Hardwood PqRow
     * @param schema the GroupType schema
     */
    public SimpleGroup(PqRow row, GroupType schema) {
        this.row = row;
        this.schema = schema;
    }

    @Override
    public GroupType getType() {
        return schema;
    }

    @Override
    public int getFieldRepetitionCount(int fieldIndex) {
        Type fieldType = schema.getType(fieldIndex);
        String fieldName = fieldType.getName();
        if (fieldType.getRepetition() == Type.Repetition.REPEATED) {
            // For repeated fields, get the list and return size
            if (row.isNull(fieldName)) {
                return 0;
            }
            PqList list = row.getList(fieldName);
            return list != null ? list.size() : 0;
        }
        // For non-repeated fields, return 1 if present, 0 if null
        return row.isNull(fieldName) ? 0 : 1;
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
            PqList list = row.getList(fieldName);
            if (list == null || index >= list.size()) {
                return null;
            }
            return (String) list.get(index);
        }
        else {
            if (index != 0) {
                throw new IndexOutOfBoundsException("Index must be 0 for non-repeated fields, got: " + index);
            }
            return row.getString(fieldName);
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
            PqList list = row.getList(fieldName);
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
            if (row.isNull(fieldName)) {
                return 0;
            }
            return row.getInt(fieldName);
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
            PqList list = row.getList(fieldName);
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
            if (row.isNull(fieldName)) {
                return 0L;
            }
            return row.getLong(fieldName);
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
            PqList list = row.getList(fieldName);
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
            if (row.isNull(fieldName)) {
                return 0.0;
            }
            return row.getDouble(fieldName);
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
            PqList list = row.getList(fieldName);
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
            if (row.isNull(fieldName)) {
                return 0.0f;
            }
            return row.getFloat(fieldName);
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
            PqList list = row.getList(fieldName);
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
            if (row.isNull(fieldName)) {
                return false;
            }
            return row.getBoolean(fieldName);
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
            PqList list = row.getList(fieldName);
            if (list == null || index >= list.size()) {
                return null;
            }
            bytes = (byte[]) list.get(index);
        }
        else {
            if (index != 0) {
                throw new IndexOutOfBoundsException("Index must be 0 for non-repeated fields, got: " + index);
            }
            bytes = row.getBinary(fieldName);
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
            // Repeated group - get from list
            PqList list = row.getList(fieldName);
            if (list == null || index >= list.size()) {
                return null;
            }
            PqRow nestedRow = (PqRow) list.get(index);
            return nestedRow != null ? new SimpleGroup(nestedRow, nestedType) : null;
        }
        else {
            // Single group
            if (index != 0) {
                throw new IndexOutOfBoundsException("Index must be 0 for non-repeated fields, got: " + index);
            }
            PqRow nestedRow = row.getRow(fieldName);
            return nestedRow != null ? new SimpleGroup(nestedRow, nestedType) : null;
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
