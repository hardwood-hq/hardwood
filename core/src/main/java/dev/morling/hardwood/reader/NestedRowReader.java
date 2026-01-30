/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import dev.morling.hardwood.internal.reader.MutableStruct;
import dev.morling.hardwood.internal.reader.NestedColumnData;
import dev.morling.hardwood.internal.reader.RecordAssembler;
import dev.morling.hardwood.internal.reader.TypedColumnData;
import dev.morling.hardwood.internal.reader.ValueConverter;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.row.PqDoubleList;
import dev.morling.hardwood.row.PqIntList;
import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqLongList;
import dev.morling.hardwood.row.PqMap;
import dev.morling.hardwood.row.PqStruct;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.SchemaNode;

/**
 * RowReader implementation for nested schemas.
 * Uses RecordAssembler to build hierarchical row structures.
 */
final class NestedRowReader extends AbstractRowReader {

    private RecordAssembler assembler;
    private List<NestedColumnData> columnData;
    private MutableStruct currentRow;

    NestedRowReader(FileSchema schema, FileChannel channel, List<RowGroup> rowGroups,
                    ExecutorService executor, String fileName) {
        super(schema, channel, rowGroups, executor, fileName);
    }

    @Override
    protected void onInitialize() {
        assembler = new RecordAssembler(schema);
    }

    @Override
    protected void onBatchLoaded(TypedColumnData[] newColumnData) {
        NestedColumnData[] nested = new NestedColumnData[newColumnData.length];
        for (int i = 0; i < newColumnData.length; i++) {
            nested[i] = (NestedColumnData) newColumnData[i];
        }
        this.columnData = List.of(nested);
    }

    @Override
    protected void onNext() {
        currentRow = assembler.assembleRow(columnData, rowIndex);
    }

    // ==================== Primitive Type Accessors ====================

    @Override
    public int getInt(String name) {
        return getInt(getFieldIndex(name));
    }

    @Override
    public int getInt(int columnIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(columnIndex);
        Integer val = ValueConverter.convertToInt(currentRow.getChild(columnIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return val;
    }

    @Override
    public long getLong(String name) {
        return getLong(getFieldIndex(name));
    }

    @Override
    public long getLong(int columnIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(columnIndex);
        Long val = ValueConverter.convertToLong(currentRow.getChild(columnIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return val;
    }

    @Override
    public float getFloat(String name) {
        return getFloat(getFieldIndex(name));
    }

    @Override
    public float getFloat(int columnIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(columnIndex);
        Float val = ValueConverter.convertToFloat(currentRow.getChild(columnIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return val;
    }

    @Override
    public double getDouble(String name) {
        return getDouble(getFieldIndex(name));
    }

    @Override
    public double getDouble(int columnIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(columnIndex);
        Double val = ValueConverter.convertToDouble(currentRow.getChild(columnIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return val;
    }

    @Override
    public boolean getBoolean(String name) {
        return getBoolean(getFieldIndex(name));
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(columnIndex);
        Boolean val = ValueConverter.convertToBoolean(currentRow.getChild(columnIndex), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return val;
    }

    // ==================== Object Type Accessors ====================

    @Override
    public String getString(String name) {
        return getString(getFieldIndex(name));
    }

    @Override
    public String getString(int columnIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(columnIndex);
        return ValueConverter.convertToString(currentRow.getChild(columnIndex), fieldSchema);
    }

    @Override
    public byte[] getBinary(String name) {
        return getBinary(getFieldIndex(name));
    }

    @Override
    public byte[] getBinary(int columnIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(columnIndex);
        return ValueConverter.convertToBinary(currentRow.getChild(columnIndex), fieldSchema);
    }

    @Override
    public LocalDate getDate(String name) {
        return getDate(getFieldIndex(name));
    }

    @Override
    public LocalDate getDate(int columnIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(columnIndex);
        return ValueConverter.convertToDate(currentRow.getChild(columnIndex), fieldSchema);
    }

    @Override
    public LocalTime getTime(String name) {
        return getTime(getFieldIndex(name));
    }

    @Override
    public LocalTime getTime(int columnIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(columnIndex);
        return ValueConverter.convertToTime(currentRow.getChild(columnIndex), fieldSchema);
    }

    @Override
    public Instant getTimestamp(String name) {
        return getTimestamp(getFieldIndex(name));
    }

    @Override
    public Instant getTimestamp(int columnIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(columnIndex);
        return ValueConverter.convertToTimestamp(currentRow.getChild(columnIndex), fieldSchema);
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return getDecimal(getFieldIndex(name));
    }

    @Override
    public BigDecimal getDecimal(int columnIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(columnIndex);
        return ValueConverter.convertToDecimal(currentRow.getChild(columnIndex), fieldSchema);
    }

    @Override
    public UUID getUuid(String name) {
        return getUuid(getFieldIndex(name));
    }

    @Override
    public UUID getUuid(int columnIndex) {
        SchemaNode fieldSchema = schema.getRootNode().children().get(columnIndex);
        return ValueConverter.convertToUuid(currentRow.getChild(columnIndex), fieldSchema);
    }

    // ==================== Nested Type Accessors ====================

    @Override
    public PqStruct getStruct(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        return ValueConverter.convertToStruct(currentRow.getChild(index), fieldSchema);
    }

    @Override
    public PqIntList getListOfInts(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        return ValueConverter.convertToIntList(currentRow.getChild(index), fieldSchema);
    }

    @Override
    public PqLongList getListOfLongs(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        return ValueConverter.convertToLongList(currentRow.getChild(index), fieldSchema);
    }

    @Override
    public PqDoubleList getListOfDoubles(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        return ValueConverter.convertToDoubleList(currentRow.getChild(index), fieldSchema);
    }

    @Override
    public PqList getList(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        return ValueConverter.convertToList(currentRow.getChild(index), fieldSchema);
    }

    @Override
    public PqMap getMap(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        return ValueConverter.convertToMap(currentRow.getChild(index), fieldSchema);
    }

    // ==================== Generic Fallback ====================

    @Override
    public Object getValue(String name) {
        return currentRow.getChild(getFieldIndex(name));
    }

    // ==================== Metadata ====================

    @Override
    public boolean isNull(String name) {
        return isNull(getFieldIndex(name));
    }

    @Override
    public boolean isNull(int columnIndex) {
        return currentRow.getChild(columnIndex) == null;
    }

    @Override
    public int getFieldCount() {
        return schema.getRootNode().children().size();
    }

    @Override
    public String getFieldName(int index) {
        return schema.getRootNode().children().get(index).name();
    }

    // ==================== Internal Helpers ====================

    private int getFieldIndex(String name) {
        List<SchemaNode> children = schema.getRootNode().children();
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Field not found: " + name);
    }
}
