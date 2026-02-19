/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.SchemaNode;

/**
 * Implementation of PqStruct interface for nested struct access.
 */
public class PqStructImpl implements PqStruct {

    private final MutableStruct values;
    private final SchemaNode.GroupNode schema;

    /**
     * Constructor for nested struct.
     */
    public PqStructImpl(MutableStruct values, SchemaNode.GroupNode structSchema) {
        this.values = values;
        this.schema = structSchema;
    }

    // ==================== Primitive Types ====================

    @Override
    public int getInt(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        Integer val = ValueConverter.convertToInt(values.getChild(index), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return val;
    }

    @Override
    public long getLong(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        Long val = ValueConverter.convertToLong(values.getChild(index), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return val;
    }

    @Override
    public float getFloat(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        Float val = ValueConverter.convertToFloat(values.getChild(index), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return val;
    }

    @Override
    public double getDouble(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        Double val = ValueConverter.convertToDouble(values.getChild(index), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return val;
    }

    @Override
    public boolean getBoolean(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        Boolean val = ValueConverter.convertToBoolean(values.getChild(index), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return val;
    }

    // ==================== Object Types ====================

    @Override
    public String getString(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convertToString(values.getChild(index), fieldSchema);
    }

    @Override
    public byte[] getBinary(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convertToBinary(values.getChild(index), fieldSchema);
    }

    @Override
    public LocalDate getDate(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convertToDate(values.getChild(index), fieldSchema);
    }

    @Override
    public LocalTime getTime(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convertToTime(values.getChild(index), fieldSchema);
    }

    @Override
    public Instant getTimestamp(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convertToTimestamp(values.getChild(index), fieldSchema);
    }

    @Override
    public BigDecimal getDecimal(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convertToDecimal(values.getChild(index), fieldSchema);
    }

    @Override
    public UUID getUuid(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convertToUuid(values.getChild(index), fieldSchema);
    }

    // ==================== Nested Types ====================

    @Override
    public PqStruct getStruct(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convertToStruct(values.getChild(index), fieldSchema);
    }

    // ==================== Primitive List Types ====================

    @Override
    public PqIntList getListOfInts(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convertToIntList(values.getChild(index), fieldSchema);
    }

    @Override
    public PqLongList getListOfLongs(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convertToLongList(values.getChild(index), fieldSchema);
    }

    @Override
    public PqDoubleList getListOfDoubles(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convertToDoubleList(values.getChild(index), fieldSchema);
    }

    // ==================== Generic List ====================

    @Override
    public PqList getList(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convertToList(values.getChild(index), fieldSchema);
    }

    @Override
    public PqMap getMap(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.children().get(index);
        return ValueConverter.convertToMap(values.getChild(index), fieldSchema);
    }

    // ==================== Generic Fallback ====================

    @Override
    public Object getValue(String name) {
        return values.getChild(getFieldIndex(name));
    }

    // ==================== Metadata ====================

    @Override
    public boolean isNull(String name) {
        return values.getChild(getFieldIndex(name)) == null;
    }

    @Override
    public int getFieldCount() {
        return schema.children().size();
    }

    @Override
    public String getFieldName(int index) {
        return schema.children().get(index).name();
    }

    private int getFieldIndex(String name) {
        List<SchemaNode> children = schema.children();
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Field not found: " + name);
    }
}
