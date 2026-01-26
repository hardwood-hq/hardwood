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
    private List<TypedColumnData> columnData;
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
        this.columnData = List.of(newColumnData);
    }

    @Override
    protected void onNext() {
        currentRow = assembler.assembleRow(columnData, rowIndex);
    }

    // ==================== Primitive Type Accessors ====================

    @Override
    public int getInt(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        Integer val = ValueConverter.convertToInt(currentRow.getChild(index), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return val;
    }

    @Override
    public long getLong(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        Long val = ValueConverter.convertToLong(currentRow.getChild(index), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return val;
    }

    @Override
    public float getFloat(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        Float val = ValueConverter.convertToFloat(currentRow.getChild(index), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return val;
    }

    @Override
    public double getDouble(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        Double val = ValueConverter.convertToDouble(currentRow.getChild(index), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return val;
    }

    @Override
    public boolean getBoolean(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        Boolean val = ValueConverter.convertToBoolean(currentRow.getChild(index), fieldSchema);
        if (val == null) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return val;
    }

    // ==================== Object Type Accessors ====================

    @Override
    public String getString(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        return ValueConverter.convertToString(currentRow.getChild(index), fieldSchema);
    }

    @Override
    public byte[] getBinary(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        return ValueConverter.convertToBinary(currentRow.getChild(index), fieldSchema);
    }

    @Override
    public LocalDate getDate(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        return ValueConverter.convertToDate(currentRow.getChild(index), fieldSchema);
    }

    @Override
    public LocalTime getTime(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        return ValueConverter.convertToTime(currentRow.getChild(index), fieldSchema);
    }

    @Override
    public Instant getTimestamp(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        return ValueConverter.convertToTimestamp(currentRow.getChild(index), fieldSchema);
    }

    @Override
    public BigDecimal getDecimal(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        return ValueConverter.convertToDecimal(currentRow.getChild(index), fieldSchema);
    }

    @Override
    public UUID getUuid(String name) {
        int index = getFieldIndex(name);
        SchemaNode fieldSchema = schema.getRootNode().children().get(index);
        return ValueConverter.convertToUuid(currentRow.getChild(index), fieldSchema);
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
        return currentRow.getChild(getFieldIndex(name)) == null;
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
