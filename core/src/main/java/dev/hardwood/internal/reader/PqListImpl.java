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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;

import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.SchemaNode;

/**
 * Flyweight {@link PqList} that reads list elements directly from column arrays.
 * <p>
 * Supports two modes:
 * <ul>
 *   <li><b>Leaf mode</b> ({@code subLevel == -1}): start/end are value indices.
 *       Elements are primitive values or structs accessed directly from column data.</li>
 *   <li><b>Nested mode</b> ({@code subLevel >= 0}): start/end are indices at an
 *       intermediate multi-level offset level. Elements are inner lists or maps,
 *       whose boundaries come from {@code ml[subLevel]}.</li>
 * </ul>
 */
final class PqListImpl implements PqList {

    private final NestedBatchIndex batch;
    private final TopLevelFieldMap.FieldDesc.ListOf listDesc;
    private final SchemaNode elementSchema;
    private final int start;     // inclusive index at this list's level
    private final int end;       // exclusive index at this list's level
    private final int subLevel;  // ml level for sub-element navigation, -1 for leaf

    PqListImpl(NestedBatchIndex batch, TopLevelFieldMap.FieldDesc.ListOf listDesc,
                   SchemaNode elementSchema, int start, int end, int subLevel) {
        this.batch = batch;
        this.listDesc = listDesc;
        this.elementSchema = elementSchema;
        this.start = start;
        this.end = end;
        this.subLevel = subLevel;
    }

    // ==================== Factory Methods ====================

    static PqList createGenericList(NestedBatchIndex batch,
                                    TopLevelFieldMap.FieldDesc.ListOf listDesc,
                                    int rowIndex, int valueIndex) {
        ListRange range = computeRange(batch, listDesc, rowIndex, valueIndex);
        if (range == null) {
            return null;
        }
        return new PqListImpl(batch, listDesc, listDesc.elementSchema(),
                range.start, range.end, range.subLevel);
    }

    static PqIntList createIntList(NestedBatchIndex batch,
                                   TopLevelFieldMap.FieldDesc.ListOf listDesc,
                                   int rowIndex, int valueIndex) {
        ListRange range = computeRange(batch, listDesc, rowIndex, valueIndex);
        if (range == null) {
            return null;
        }
        return new PqIntListImpl(batch, listDesc.firstLeafProjCol(),
                listDesc.elementSchema(), range.start, range.end);
    }

    static PqLongList createLongList(NestedBatchIndex batch,
                                     TopLevelFieldMap.FieldDesc.ListOf listDesc,
                                     int rowIndex, int valueIndex) {
        ListRange range = computeRange(batch, listDesc, rowIndex, valueIndex);
        if (range == null) {
            return null;
        }
        return new PqLongListImpl(batch, listDesc.firstLeafProjCol(),
                listDesc.elementSchema(), range.start, range.end);
    }

    static PqDoubleList createDoubleList(NestedBatchIndex batch,
                                         TopLevelFieldMap.FieldDesc.ListOf listDesc,
                                         int rowIndex, int valueIndex) {
        ListRange range = computeRange(batch, listDesc, rowIndex, valueIndex);
        if (range == null) {
            return null;
        }
        return new PqDoubleListImpl(batch, listDesc.firstLeafProjCol(),
                listDesc.elementSchema(), range.start, range.end);
    }

    static boolean isListNull(NestedBatchIndex batch,
                              TopLevelFieldMap.FieldDesc.ListOf listDesc,
                              int rowIndex, int valueIndex) {
        int projCol = listDesc.firstLeafProjCol();
        int valIdx = resolveFirstValueIndex(batch, listDesc, rowIndex, valueIndex);
        int defLevel = batch.columns[projCol].getDefLevel(valIdx);
        return defLevel < listDesc.nullDefLevel();
    }

    // ==================== PqList Interface ====================

    @Override
    public int size() {
        return end - start;
    }

    @Override
    public boolean isEmpty() {
        return start >= end;
    }

    @Override
    public Object get(int index) {
        checkBounds(index);
        if (subLevel >= 0) {
            return getNestedElement(index);
        }
        // Leaf level: check if element is a nested type (struct/list/map within a leaf-level list)
        if (elementSchema instanceof SchemaNode.GroupNode group) {
            if (group.isStruct()) {
                return createInnerStruct(index);
            } else if (group.isList()) {
                return createInnerGenericList(index);
            } else if (group.isMap()) {
                return createInnerMap(index);
            }
        }
        return getLeafValue(index);
    }

    @Override
    public boolean isNull(int index) {
        checkBounds(index);
        if (subLevel < 0) {
            return batch.isElementNull(listDesc.firstLeafProjCol(), start + index);
        }
        return isNestedElementNull(index);
    }

    @Override
    public Iterable<Object> values() {
        if (elementSchema instanceof SchemaNode.GroupNode) {
            // For nested types, use index-based access
            return () -> new NestedListIterator<>(i -> get(i));
        }
        return () -> new LeafIterator<>(raw -> ValueConverter.convertValue(raw, elementSchema));
    }

    // ==================== Primitive Type Accessors ====================

    @Override
    public Iterable<Integer> ints() {
        return () -> new LeafIterator<>(raw -> ValueConverter.convertToInt(raw, elementSchema));
    }

    @Override
    public Iterable<Long> longs() {
        return () -> new LeafIterator<>(raw -> ValueConverter.convertToLong(raw, elementSchema));
    }

    @Override
    public Iterable<Float> floats() {
        return () -> new LeafIterator<>(raw -> ValueConverter.convertToFloat(raw, elementSchema));
    }

    @Override
    public Iterable<Double> doubles() {
        return () -> new LeafIterator<>(raw -> ValueConverter.convertToDouble(raw, elementSchema));
    }

    @Override
    public Iterable<Boolean> booleans() {
        return () -> new LeafIterator<>(raw -> ValueConverter.convertToBoolean(raw, elementSchema));
    }

    // ==================== Object Type Accessors ====================

    @Override
    public Iterable<String> strings() {
        return () -> new LeafIterator<>(raw -> ValueConverter.convertToString(raw, elementSchema));
    }

    @Override
    public Iterable<byte[]> binaries() {
        return () -> new LeafIterator<>(raw -> ValueConverter.convertToBinary(raw, elementSchema));
    }

    @Override
    public Iterable<LocalDate> dates() {
        return () -> new LeafIterator<>(raw -> ValueConverter.convertToDate(raw, elementSchema));
    }

    @Override
    public Iterable<LocalTime> times() {
        return () -> new LeafIterator<>(raw -> ValueConverter.convertToTime(raw, elementSchema));
    }

    @Override
    public Iterable<Instant> timestamps() {
        return () -> new LeafIterator<>(raw -> ValueConverter.convertToTimestamp(raw, elementSchema));
    }

    @Override
    public Iterable<BigDecimal> decimals() {
        return () -> new LeafIterator<>(raw -> ValueConverter.convertToDecimal(raw, elementSchema));
    }

    @Override
    public Iterable<UUID> uuids() {
        return () -> new LeafIterator<>(raw -> ValueConverter.convertToUuid(raw, elementSchema));
    }

    // ==================== Nested Type Accessors ====================

    @Override
    public Iterable<PqStruct> structs() {
        return () -> new StructIterator();
    }

    @Override
    public Iterable<PqList> lists() {
        return () -> new NestedListIterator<>(this::createInnerGenericList);
    }

    @Override
    public Iterable<PqIntList> intLists() {
        return () -> new NestedListIterator<>(this::createInnerIntList);
    }

    @Override
    public Iterable<PqLongList> longLists() {
        return () -> new NestedListIterator<>(this::createInnerLongList);
    }

    @Override
    public Iterable<PqDoubleList> doubleLists() {
        return () -> new NestedListIterator<>(this::createInnerDoubleList);
    }

    @Override
    public Iterable<PqMap> maps() {
        return () -> new NestedListIterator<>(this::createInnerMap);
    }

    // ==================== Internal: Range Computation ====================

    private record ListRange(int start, int end, int subLevel) {}

    private static ListRange computeRange(NestedBatchIndex batch,
                                          TopLevelFieldMap.FieldDesc.ListOf listDesc,
                                          int rowIndex, int valueIndex) {
        int projCol = listDesc.firstLeafProjCol();
        int mlLevel = listDesc.schema().maxRepetitionLevel();
        int leafMaxRep = batch.columns[projCol].column().maxRepetitionLevel();

        int start, end;
        if (valueIndex >= 0 && mlLevel > 0) {
            // Position mode: list inside a struct inside an ancestor list
            start = batch.getLevelStart(projCol, mlLevel, valueIndex);
            end = batch.getLevelEnd(projCol, mlLevel, valueIndex);
        } else {
            // Record mode
            start = batch.getListStart(projCol, rowIndex);
            end = batch.getListEnd(projCol, rowIndex);
        }

        // Check null/empty using defLevel at the first value position
        int firstValueIdx = resolveFirstValue(batch, projCol, start, mlLevel, leafMaxRep);
        int defLevel = batch.columns[projCol].getDefLevel(firstValueIdx);
        if (defLevel < listDesc.nullDefLevel()) {
            return null; // null list
        }

        int subLevel = (mlLevel < leafMaxRep - 1) ? mlLevel + 1 : -1;

        if (defLevel < listDesc.elementDefLevel()) {
            // Empty list
            return new ListRange(start, start, subLevel);
        }

        return new ListRange(start, end, subLevel);
    }

    private static int resolveFirstValueIndex(NestedBatchIndex batch,
                                              TopLevelFieldMap.FieldDesc.ListOf listDesc,
                                              int rowIndex, int valueIndex) {
        int projCol = listDesc.firstLeafProjCol();
        int mlLevel = listDesc.schema().maxRepetitionLevel();
        int leafMaxRep = batch.columns[projCol].column().maxRepetitionLevel();

        int start;
        if (valueIndex >= 0 && mlLevel > 0) {
            start = batch.getLevelStart(projCol, mlLevel, valueIndex);
        } else {
            start = batch.getListStart(projCol, rowIndex);
        }
        return resolveFirstValue(batch, projCol, start, mlLevel, leafMaxRep);
    }

    private static int resolveFirstValue(NestedBatchIndex batch, int projCol,
                                         int start, int mlLevel, int leafMaxRep) {
        // Chase through ml levels to get an actual value index
        int idx = start;
        for (int level = mlLevel + 1; level < leafMaxRep; level++) {
            idx = batch.getLevelStart(projCol, level, idx);
        }
        return idx;
    }

    // ==================== Internal: Element Access ====================

    private Object getLeafValue(int index) {
        int projCol = listDesc.firstLeafProjCol();
        int valueIdx = start + index;
        if (batch.isElementNull(projCol, valueIdx)) {
            return null;
        }
        return ValueConverter.convertValue(batch.columns[projCol].getValue(valueIdx), elementSchema);
    }

    private Object getNestedElement(int index) {
        if (elementSchema instanceof SchemaNode.GroupNode group) {
            if (group.isList()) {
                return createInnerGenericList(index);
            } else if (group.isMap()) {
                return createInnerMap(index);
            } else {
                return createInnerStruct(index);
            }
        }
        // Should not happen for nested mode
        return getLeafValue(index);
    }

    private boolean isNestedElementNull(int index) {
        if (!(elementSchema instanceof SchemaNode.GroupNode group)) {
            return false;
        }
        int projCol = listDesc.firstLeafProjCol();
        int itemIndex = start + index;
        int firstValue = resolveFirstValue(batch, projCol, itemIndex, subLevel - 1,
                batch.columns[projCol].column().maxRepetitionLevel());
        int defLevel = batch.columns[projCol].getDefLevel(firstValue);

        if (group.isList()) {
            return defLevel < group.maxDefinitionLevel();
        } else if (group.isMap()) {
            return defLevel < group.maxDefinitionLevel();
        } else {
            return defLevel < group.maxDefinitionLevel();
        }
    }

    // ==================== Internal: Inner List/Struct Creation ====================

    private PqStruct createInnerStruct(int index) {
        int valueIdx = start + index;
        if (!(elementSchema instanceof SchemaNode.GroupNode group) || !group.isStruct()) {
            throw new IllegalArgumentException("Element is not a struct");
        }
        TopLevelFieldMap.FieldDesc.Struct structDesc =
                DescriptorBuilder.buildStructDesc(group, batch.projectedSchema);
        if (isStructElementNull(structDesc, valueIdx)) {
            return null;
        }
        return PqStructImpl.atPosition(batch, structDesc, valueIdx);
    }

    private PqList createInnerGenericList(int index) {
        if (!(elementSchema instanceof SchemaNode.GroupNode group) || !group.isList()) {
            throw new IllegalArgumentException("Element is not a list");
        }
        int projCol = listDesc.firstLeafProjCol();
        int itemIndex = start + index;
        int innerStart = batch.getLevelStart(projCol, subLevel, itemIndex);
        int innerEnd = batch.getLevelEnd(projCol, subLevel, itemIndex);

        SchemaNode innerElement = group.getListElement();
        int nullDef = group.maxDefinitionLevel();
        SchemaNode innerRepeated = group.children().get(0);
        int elemDef = innerRepeated.maxDefinitionLevel();

        // Check null/empty
        int leafMaxRep = batch.columns[projCol].column().maxRepetitionLevel();
        int innerSubLevel = (subLevel < leafMaxRep - 1) ? subLevel + 1 : -1;
        int firstValue = resolveFirstValue(batch, projCol, innerStart,
                subLevel, leafMaxRep);
        int defLevel = batch.columns[projCol].getDefLevel(firstValue);
        if (defLevel < nullDef) {
            return null;
        }
        if (defLevel < elemDef) {
            return new PqListImpl(batch, listDesc, innerElement,
                    innerStart, innerStart, innerSubLevel);
        }
        return new PqListImpl(batch, listDesc, innerElement,
                innerStart, innerEnd, innerSubLevel);
    }

    private PqIntList createInnerIntList(int index) {
        PqList inner = createInnerGenericList(index);
        if (inner == null) {
            return null;
        }
        PqListImpl innerList = (PqListImpl) inner;
        return new PqIntListImpl(batch, listDesc.firstLeafProjCol(),
                innerList.elementSchema, innerList.start, innerList.end);
    }

    private PqLongList createInnerLongList(int index) {
        PqList inner = createInnerGenericList(index);
        if (inner == null) {
            return null;
        }
        PqListImpl innerList = (PqListImpl) inner;
        return new PqLongListImpl(batch, listDesc.firstLeafProjCol(),
                innerList.elementSchema, innerList.start, innerList.end);
    }

    private PqDoubleList createInnerDoubleList(int index) {
        PqList inner = createInnerGenericList(index);
        if (inner == null) {
            return null;
        }
        PqListImpl innerList = (PqListImpl) inner;
        return new PqDoubleListImpl(batch, listDesc.firstLeafProjCol(),
                innerList.elementSchema, innerList.start, innerList.end);
    }

    private PqMap createInnerMap(int index) {
        if (!(elementSchema instanceof SchemaNode.GroupNode group) || !group.isMap()) {
            throw new IllegalArgumentException("Element is not a map");
        }
        int valueIdx = start + index;
        TopLevelFieldMap.FieldDesc.MapOf innerMapDesc =
                DescriptorBuilder.buildMapDesc(group, batch.projectedSchema);
        return PqMapImpl.create(batch, innerMapDesc, -1, valueIdx);
    }

    private boolean isStructElementNull(TopLevelFieldMap.FieldDesc.Struct structDesc, int valueIdx) {
        for (TopLevelFieldMap.FieldDesc childDesc : structDesc.children().values()) {
            if (childDesc instanceof TopLevelFieldMap.FieldDesc.Primitive p) {
                int projCol = p.projectedCol();
                NestedColumnData data = batch.columns[projCol];
                int defLevel = data.getDefLevel(valueIdx);
                int structDefLevel = structDesc.schema().maxDefinitionLevel();
                return defLevel < structDefLevel;
            }
        }
        return false;
    }

    // ==================== Internal: Bounds Check ====================

    private void checkBounds(int index) {
        if (index < 0 || index >= size()) {
            throw new IndexOutOfBoundsException("Index " + index + " out of range [0, " + size() + ")");
        }
    }

    // ==================== Internal: Iterators ====================

    private class LeafIterator<T> implements Iterator<T> {
        private final java.util.function.Function<Object, T> converter;
        private int pos = start;

        LeafIterator(java.util.function.Function<Object, T> converter) {
            this.converter = converter;
        }

        @Override
        public boolean hasNext() {
            return pos < end;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            int projCol = listDesc.firstLeafProjCol();
            if (batch.isElementNull(projCol, pos)) {
                pos++;
                return null;
            }
            Object raw = batch.columns[projCol].getValue(pos++);
            return converter.apply(raw);
        }
    }

    private class StructIterator implements Iterator<PqStruct> {
        private int pos = 0;

        @Override
        public boolean hasNext() {
            return pos < size();
        }

        @Override
        public PqStruct next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return createInnerStruct(pos++);
        }
    }

    private class NestedListIterator<T> implements Iterator<T> {
        private final java.util.function.IntFunction<T> creator;
        private int pos = 0;

        NestedListIterator(java.util.function.IntFunction<T> creator) {
            this.creator = creator;
        }

        @Override
        public boolean hasNext() {
            return pos < size();
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return creator.apply(pos++);
        }
    }
}
