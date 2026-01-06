/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import dev.morling.hardwood.internal.conversion.LogicalTypeConverter;
import dev.morling.hardwood.row.Row;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.SchemaNode;

/**
 * Implementation of Row interface backed by an Object array or Map for nested structures.
 */
public class RowImpl implements Row {

    private final Object[] values;
    private final Map<String, Object> nestedValues;
    private final FileSchema schema;
    private final SchemaNode.GroupNode structSchema;

    /**
     * Constructor for flat rows (array-backed).
     */
    public RowImpl(Object[] values, FileSchema schema) {
        this.values = values;
        this.nestedValues = null;
        this.schema = schema;
        this.structSchema = null;
    }

    /**
     * Constructor for nested struct rows (map-backed).
     */
    public RowImpl(Map<String, Object> nestedValues, SchemaNode.GroupNode structSchema) {
        this.values = null;
        this.nestedValues = nestedValues;
        this.schema = null;
        this.structSchema = structSchema;
    }

    @Override
    public boolean getBoolean(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }
        return (Boolean) value;
    }

    @Override
    public boolean getBoolean(String name) {
        return getBoolean(getColumnIndex(name));
    }

    @Override
    public int getInt(int position) {
        Object value = values != null ? values[position] : getNestedValue(position);
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }
        return (Integer) value;
    }

    @Override
    public int getInt(String name) {
        if (nestedValues != null) {
            Object value = nestedValues.get(name);
            if (value == null) {
                throw new NullPointerException("Field " + name + " is null");
            }
            return (Integer) value;
        }
        return getInt(getColumnIndex(name));
    }

    @Override
    public long getLong(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }
        return (Long) value;
    }

    @Override
    public long getLong(String name) {
        return getLong(getColumnIndex(name));
    }

    @Override
    public float getFloat(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }
        return (Float) value;
    }

    @Override
    public float getFloat(String name) {
        return getFloat(getColumnIndex(name));
    }

    @Override
    public double getDouble(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }
        return (Double) value;
    }

    @Override
    public double getDouble(String name) {
        return getDouble(getColumnIndex(name));
    }

    @Override
    public byte[] getByteArray(int position) {
        return (byte[]) values[position];
    }

    @Override
    public byte[] getByteArray(String name) {
        return getByteArray(getColumnIndex(name));
    }

    @Override
    public String getString(int position) {
        Object value = values != null ? values[position] : getNestedValue(position);
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        return new String((byte[]) value, StandardCharsets.UTF_8);
    }

    @Override
    public String getString(String name) {
        if (nestedValues != null) {
            Object value = nestedValues.get(name);
            if (value == null) {
                return null;
            }
            if (value instanceof String) {
                return (String) value;
            }
            return new String((byte[]) value, StandardCharsets.UTF_8);
        }
        return getString(getColumnIndex(name));
    }

    @Override
    public LocalDate getDate(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }

        ColumnSchema column = schema.getColumn(position);
        Object converted = LogicalTypeConverter.convert(value, column.type(), column.logicalType());

        if (!(converted instanceof LocalDate)) {
            throw new ClassCastException("Column " + position + " (" + column.name() + ") is not a DATE type");
        }
        return (LocalDate) converted;
    }

    @Override
    public LocalDate getDate(String name) {
        return getDate(getColumnIndex(name));
    }

    @Override
    public LocalTime getTime(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }

        ColumnSchema column = schema.getColumn(position);
        Object converted = LogicalTypeConverter.convert(value, column.type(), column.logicalType());

        if (!(converted instanceof LocalTime)) {
            throw new ClassCastException("Column " + position + " (" + column.name() + ") is not a TIME type");
        }
        return (LocalTime) converted;
    }

    @Override
    public LocalTime getTime(String name) {
        return getTime(getColumnIndex(name));
    }

    @Override
    public Instant getTimestamp(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }

        ColumnSchema column = schema.getColumn(position);
        Object converted = LogicalTypeConverter.convert(value, column.type(), column.logicalType());

        if (!(converted instanceof Instant)) {
            throw new ClassCastException("Column " + position + " (" + column.name() + ") is not a TIMESTAMP type");
        }
        return (Instant) converted;
    }

    @Override
    public Instant getTimestamp(String name) {
        return getTimestamp(getColumnIndex(name));
    }

    @Override
    public BigDecimal getDecimal(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }

        ColumnSchema column = schema.getColumn(position);
        Object converted = LogicalTypeConverter.convert(value, column.type(), column.logicalType());

        if (!(converted instanceof BigDecimal)) {
            throw new ClassCastException("Column " + position + " (" + column.name() + ") is not a DECIMAL type");
        }
        return (BigDecimal) converted;
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return getDecimal(getColumnIndex(name));
    }

    @Override
    public UUID getUuid(int position) {
        Object value = values[position];
        if (value == null) {
            throw new NullPointerException("Column " + position + " is null");
        }

        ColumnSchema column = schema.getColumn(position);
        Object converted = LogicalTypeConverter.convert(value, column.type(), column.logicalType());

        if (!(converted instanceof UUID)) {
            throw new ClassCastException("Column " + position + " (" + column.name() + ") is not a UUID type");
        }
        return (UUID) converted;
    }

    @Override
    public UUID getUuid(String name) {
        return getUuid(getColumnIndex(name));
    }

    @Override
    public Object getObject(int position) {
        Object value = values[position];
        if (value == null) {
            return null;
        }

        ColumnSchema column = schema.getColumn(position);
        return LogicalTypeConverter.convert(value, column.type(), column.logicalType());
    }

    @Override
    public Object getObject(String name) {
        return getObject(getColumnIndex(name));
    }

    @Override
    public boolean isNull(int position) {
        return values[position] == null;
    }

    @Override
    public boolean isNull(String name) {
        return isNull(getColumnIndex(name));
    }

    @Override
    public int getColumnCount() {
        return schema.getColumnCount();
    }

    @Override
    public String getColumnName(int position) {
        return schema.getColumn(position).name();
    }

    private int getColumnIndex(String name) {
        return schema.getColumn(name).columnIndex();
    }

    /**
     * Get value by name for nested rows.
     */
    private Object getNestedValue(String name) {
        if (nestedValues == null) {
            throw new IllegalStateException("Not a nested row");
        }
        return nestedValues.get(name);
    }

    /**
     * Get value by position for nested rows.
     */
    private Object getNestedValue(int position) {
        if (structSchema == null) {
            throw new IllegalStateException("Not a nested row");
        }
        String name = structSchema.children().get(position).name();
        return nestedValues.get(name);
    }

    /**
     * Get field index by name for nested rows.
     */
    private int getNestedFieldIndex(String name) {
        if (structSchema == null) {
            throw new IllegalStateException("Not a nested row");
        }
        for (int i = 0; i < structSchema.children().size(); i++) {
            if (structSchema.children().get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Field not found: " + name);
    }

    /**
     * Get field schema by name for nested rows.
     */
    private SchemaNode getNestedFieldSchema(String name) {
        if (structSchema == null) {
            throw new IllegalStateException("Not a nested row");
        }
        for (SchemaNode child : structSchema.children()) {
            if (child.name().equals(name)) {
                return child;
            }
        }
        throw new IllegalArgumentException("Field not found: " + name);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Row getStruct(int position) {
        Object value;
        SchemaNode.GroupNode fieldSchema;

        if (values != null) {
            // Flat row - get from values array
            value = values[position];
            SchemaNode node = schema.getRootNode().children().get(position);
            if (!(node instanceof SchemaNode.GroupNode gn) || gn.isList()) {
                throw new IllegalArgumentException("Position " + position + " is not a struct");
            }
            fieldSchema = gn;
        }
        else {
            // Nested row - get from map
            value = getNestedValue(position);
            SchemaNode node = structSchema.children().get(position);
            if (!(node instanceof SchemaNode.GroupNode gn) || gn.isList()) {
                throw new IllegalArgumentException("Position " + position + " is not a struct");
            }
            fieldSchema = gn;
        }

        if (value == null) {
            return null;
        }
        return new RowImpl((Map<String, Object>) value, fieldSchema);
    }

    @Override
    public Row getStruct(String name) {
        if (values != null) {
            SchemaNode node = schema.getField(name);
            if (!(node instanceof SchemaNode.GroupNode gn) || gn.isList()) {
                throw new IllegalArgumentException("Field " + name + " is not a struct");
            }
            int position = getFieldPosition(name);
            return getStruct(position);
        }
        else {
            return getStruct(getNestedFieldIndex(name));
        }
    }

    /**
     * Get position of a top-level field by name.
     */
    private int getFieldPosition(String name) {
        List<SchemaNode> children = schema.getRootNode().children();
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Field not found: " + name);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Integer> getIntList(int position) {
        Object value = values != null ? values[position] : getNestedValue(position);
        if (value == null) {
            return null;
        }
        return (List<Integer>) value;
    }

    @Override
    public List<Integer> getIntList(String name) {
        if (values != null) {
            return getIntList(getFieldPosition(name));
        }
        else {
            return getIntList(getNestedFieldIndex(name));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Long> getLongList(int position) {
        Object value = values != null ? values[position] : getNestedValue(position);
        if (value == null) {
            return null;
        }
        return (List<Long>) value;
    }

    @Override
    public List<Long> getLongList(String name) {
        if (values != null) {
            return getLongList(getFieldPosition(name));
        }
        else {
            return getLongList(getNestedFieldIndex(name));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String> getStringList(int position) {
        Object value = values != null ? values[position] : getNestedValue(position);
        if (value == null) {
            return null;
        }

        // Convert byte[] to String if needed
        List<?> rawList = (List<?>) value;
        if (rawList.isEmpty()) {
            return new ArrayList<>();
        }

        if (rawList.get(0) instanceof byte[]) {
            List<String> result = new ArrayList<>();
            for (Object item : rawList) {
                if (item == null) {
                    result.add(null);
                }
                else {
                    result.add(new String((byte[]) item, StandardCharsets.UTF_8));
                }
            }
            return result;
        }
        return (List<String>) value;
    }

    @Override
    public List<String> getStringList(String name) {
        if (values != null) {
            return getStringList(getFieldPosition(name));
        }
        else {
            return getStringList(getNestedFieldIndex(name));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Row> getStructList(int position) {
        Object value = values != null ? values[position] : getNestedValue(position);
        if (value == null) {
            return null;
        }

        // Get the element schema for the list
        SchemaNode.GroupNode listSchema;
        if (values != null) {
            SchemaNode node = schema.getRootNode().children().get(position);
            if (!(node instanceof SchemaNode.GroupNode gn) || !gn.isList()) {
                throw new IllegalArgumentException("Position " + position + " is not a list");
            }
            listSchema = gn;
        }
        else {
            SchemaNode node = structSchema.children().get(position);
            if (!(node instanceof SchemaNode.GroupNode gn) || !gn.isList()) {
                throw new IllegalArgumentException("Position " + position + " is not a list");
            }
            listSchema = gn;
        }

        SchemaNode elementNode = listSchema.getListElement();
        if (!(elementNode instanceof SchemaNode.GroupNode elementGroup) || elementGroup.isList()) {
            throw new IllegalArgumentException("List elements are not structs");
        }

        List<Map<String, Object>> rawList = (List<Map<String, Object>>) value;
        List<Row> result = new ArrayList<>();
        for (Map<String, Object> item : rawList) {
            if (item == null) {
                result.add(null);
            }
            else {
                result.add(new RowImpl(item, elementGroup));
            }
        }
        return result;
    }

    @Override
    public List<Row> getStructList(String name) {
        if (values != null) {
            return getStructList(getFieldPosition(name));
        }
        else {
            return getStructList(getNestedFieldIndex(name));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<?> getList(int position) {
        Object value = values != null ? values[position] : getNestedValue(position);
        if (value == null) {
            return null;
        }
        return (List<?>) value;
    }

    @Override
    public List<?> getList(String name) {
        if (values != null) {
            return getList(getFieldPosition(name));
        }
        else {
            return getList(getNestedFieldIndex(name));
        }
    }
}
