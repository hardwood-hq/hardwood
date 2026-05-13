/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.IntFunction;

import dev.hardwood.internal.reader.TopLevelFieldMap.FieldDesc.ListOf;
import dev.hardwood.internal.variant.PqVariantImpl;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.SchemaNode;

/// Flyweight [PqList] that reads list elements directly from column arrays.
///
/// Supports two modes:
///
/// - **Leaf mode** (`subLevel == -1`): start/end are value indices.
///       Elements are primitive values or structs accessed directly from column data.</li>
/// - **Nested mode** (`subLevel >= 0`): start/end are indices at an
///       intermediate multi-level offset level. Elements are inner lists or maps,
///       whose boundaries come from `ml[subLevel]`.</li>
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

    static boolean isListNull(NestedBatchIndex batch,
                              TopLevelFieldMap.FieldDesc.ListOf listDesc,
                              int rowIndex, int valueIndex) {
        int projCol = listDesc.firstLeafProjCol();
        int valIdx = resolveFirstValueIndex(batch, listDesc, rowIndex, valueIndex);
        int defLevel = batch.getDefLevel(projCol, valIdx);
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
        // Leaf level: check if element is a nested type (struct/list/map/variant within a leaf-level list)
        if (elementSchema instanceof SchemaNode.GroupNode group) {
            if (group.isVariant()) {
                return createInnerVariant(index);
            } else if (group.isStruct()) {
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
            // Leaf-level indexing. If the element is a nested group (struct/list/map),
            // nullness is governed by the group's own definition level, not the leaf
            // column's element-null bitmap (which only flags a null primitive value).
            if (elementSchema instanceof SchemaNode.GroupNode group) {
                int projCol = listDesc.firstLeafProjCol();
                int defLevel = batch.getDefLevel(projCol, start + index);
                return defLevel < group.maxDefinitionLevel();
            }
            return batch.isElementNull(listDesc.firstLeafProjCol(), start + index);
        }
        return isNestedElementNull(index);
    }

    @Override
    public List<Object> values() {
        if (elementSchema instanceof SchemaNode.GroupNode) {
            return new NestedList<>(this::get);
        }
        return new LeafList<>(raw -> ValueConverter.convertValue(raw, elementSchema));
    }

    @Override
    public Object getRaw(int index) {
        checkBounds(index);
        if (subLevel >= 0) {
            // Nested-mode elements are always groups — same flyweight as get().
            return getNestedElement(index);
        }
        if (elementSchema instanceof SchemaNode.GroupNode group) {
            if (group.isVariant()) {
                return createInnerVariant(index);
            } else if (group.isStruct()) {
                return createInnerStruct(index);
            } else if (group.isList()) {
                return createInnerGenericList(index);
            } else if (group.isMap()) {
                return createInnerMap(index);
            }
        }
        int projCol = listDesc.firstLeafProjCol();
        int valueIdx = start + index;
        if (batch.isElementNull(projCol, valueIdx)) {
            return null;
        }
        return batch.getValue(projCol, valueIdx);
    }

    @Override
    public List<Object> rawValues() {
        if (elementSchema instanceof SchemaNode.GroupNode) {
            return new NestedList<>(this::getRaw);
        }
        return new LeafList<>(raw -> raw);
    }

    // ==================== Primitive Type Accessors ====================
    //
    // Each accessor validates `elementSchema` once at construction so the
    // per-element decode reduces to a cast (primitives) or a delegate to
    // `convertLogicalType` (logical-typed objects). Wrong-type access
    // throws field-named `IllegalArgumentException` from the accessor
    // call rather than from the first element decode, so
    // `pqList.dates()` on a STRING list fails fast even when the list
    // happens to be empty.

    @Override
    public PqIntList ints() {
        ValueConverter.validatePhysicalType(elementSchema, PhysicalType.INT32);
        return new PqIntListImpl(batch, listDesc.firstLeafProjCol(), start, end);
    }

    @Override
    public PqLongList longs() {
        ValueConverter.validatePhysicalType(elementSchema, PhysicalType.INT64);
        return new PqLongListImpl(batch, listDesc.firstLeafProjCol(), start, end);
    }

    @Override
    public List<Float> floats() {
        // FLOAT16 (FLBA(2) + Float16Type) decodes per element via the typed
        // converter; plain FLOAT is a direct cast.
        if (elementSchema instanceof SchemaNode.PrimitiveNode prim
                && prim.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY
                && prim.logicalType() instanceof LogicalType.Float16Type) {
            return new LeafList<>(raw -> ValueConverter.convertLogicalType(raw, elementSchema, Float.class));
        }
        ValueConverter.validatePhysicalType(elementSchema, PhysicalType.FLOAT);
        return new LeafList<>(raw -> (Float) raw);
    }

    @Override
    public PqDoubleList doubles() {
        ValueConverter.validatePhysicalType(elementSchema, PhysicalType.DOUBLE);
        return new PqDoubleListImpl(batch, listDesc.firstLeafProjCol(), start, end);
    }

    @Override
    public List<Boolean> booleans() {
        ValueConverter.validatePhysicalType(elementSchema, PhysicalType.BOOLEAN);
        return new LeafList<>(raw -> (Boolean) raw);
    }

    // ==================== Object Type Accessors ====================

    @Override
    public List<String> strings() {
        ValueConverter.validatePhysicalType(elementSchema, PhysicalType.BYTE_ARRAY);
        return new LeafList<>(raw -> {
            if (raw instanceof String s) return s;
            return new String((byte[]) raw, StandardCharsets.UTF_8);
        });
    }

    @Override
    public List<byte[]> binaries() {
        ValueConverter.validatePhysicalType(elementSchema, PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY);
        return new LeafList<>(raw -> (byte[]) raw);
    }

    @Override
    public List<LocalDate> dates() {
        ValueConverter.validateLogicalType(elementSchema, LogicalType.DateType.class);
        return new LeafList<>(raw -> ValueConverter.convertLogicalType(raw, elementSchema, LocalDate.class));
    }

    @Override
    public List<LocalTime> times() {
        ValueConverter.validateLogicalType(elementSchema, LogicalType.TimeType.class);
        return new LeafList<>(raw -> ValueConverter.convertLogicalType(raw, elementSchema, LocalTime.class));
    }

    @Override
    public List<Instant> timestamps() {
        ValueConverter.validateLogicalType(elementSchema, LogicalType.TimestampType.class);
        return new LeafList<>(raw -> ValueConverter.convertLogicalType(raw, elementSchema, Instant.class));
    }

    @Override
    public List<BigDecimal> decimals() {
        ValueConverter.validateLogicalType(elementSchema, LogicalType.DecimalType.class);
        return new LeafList<>(raw -> ValueConverter.convertLogicalType(raw, elementSchema, BigDecimal.class));
    }

    @Override
    public List<UUID> uuids() {
        ValueConverter.validateLogicalType(elementSchema, LogicalType.UuidType.class);
        return new LeafList<>(raw -> ValueConverter.convertLogicalType(raw, elementSchema, UUID.class));
    }

    @Override
    public List<PqInterval> intervals() {
        ValueConverter.validateLogicalType(elementSchema, LogicalType.IntervalType.class);
        return new LeafList<>(raw -> ValueConverter.convertLogicalType(raw, elementSchema, PqInterval.class));
    }

    // ==================== Nested Type Accessors ====================

    @Override
    public List<PqStruct> structs() {
        return new NestedList<>(this::createInnerStruct);
    }

    @Override
    public List<PqList> lists() {
        return new NestedList<>(this::createInnerGenericList);
    }

    @Override
    public List<PqMap> maps() {
        return new NestedList<>(this::createInnerMap);
    }

    @Override
    public List<PqVariant> variants() {
        return new NestedList<>(this::createInnerVariant);
    }

    // ==================== Internal: Range Computation ====================

    private record ListRange(int start, int end, int subLevel) {}

    private static ListRange computeRange(NestedBatchIndex batch,
                                          TopLevelFieldMap.FieldDesc.ListOf listDesc,
                                          int rowIndex, int valueIndex) {
        int projCol = listDesc.firstLeafProjCol();
        int mlLevel = listDesc.schema().maxRepetitionLevel();
        int leafMaxRep = batch.getMaxRepLevel(projCol);

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
        int defLevel = batch.getDefLevel(projCol, firstValueIdx);
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
        int leafMaxRep = batch.getMaxRepLevel(projCol);

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
        return ValueConverter.convertValue(batch.getValue(projCol, valueIdx), elementSchema);
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
                batch.getMaxRepLevel(projCol));
        int defLevel = batch.getDefLevel(projCol, firstValue);

        return defLevel < group.maxDefinitionLevel();
    }

    // ==================== Internal: Inner List/Struct Creation ====================

    private PqStruct createInnerStruct(int index) {
        int valueIdx = start + index;
        TopLevelFieldMap.FieldDesc elementDesc = listDesc.elementDesc();
        if (!(elementDesc instanceof TopLevelFieldMap.FieldDesc.Struct structDesc)) {
            throw new IllegalArgumentException("Element is not a struct");
        }
        if (isStructElementNull(structDesc, valueIdx)) {
            return null;
        }
        return PqStructImpl.atPosition(batch, structDesc, valueIdx);
    }

    private PqList createInnerGenericList(int index) {
        if (!(elementSchema instanceof SchemaNode.GroupNode group) || !group.isList()) {
            throw new IllegalArgumentException("Element is not a list");
        }
        // For nested lists (list<list<...>>), the inner list needs its own descriptor so
        // that subsequent `.structs()` / `.maps()` calls resolve the correct element
        // metadata. The outer list's pre-built `elementDesc` is the descriptor for the
        // inner list; fall back to building one if it was not cached.
        ListOf innerListDesc;
        if (listDesc.elementDesc() instanceof ListOf cached) {
            innerListDesc = cached;
        } else {
            innerListDesc = DescriptorBuilder.buildListDesc(group, batch.projectedSchema);
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
        int leafMaxRep = batch.getMaxRepLevel(projCol);
        int innerSubLevel = (subLevel < leafMaxRep - 1) ? subLevel + 1 : -1;
        int firstValue = resolveFirstValue(batch, projCol, innerStart,
                subLevel, leafMaxRep);
        int defLevel = batch.getDefLevel(projCol, firstValue);
        if (defLevel < nullDef) {
            return null;
        }
        if (defLevel < elemDef) {
            return new PqListImpl(batch, innerListDesc, innerElement,
                    innerStart, innerStart, innerSubLevel);
        }
        return new PqListImpl(batch, innerListDesc, innerElement,
                innerStart, innerEnd, innerSubLevel);
    }

    private PqMap createInnerMap(int index) {
        if (!(elementSchema instanceof SchemaNode.GroupNode group) || !group.isMap()) {
            throw new IllegalArgumentException("Element is not a map");
        }
        int valueIdx = start + index;
        TopLevelFieldMap.FieldDesc elementDesc = listDesc.elementDesc();
        if (!(elementDesc instanceof TopLevelFieldMap.FieldDesc.MapOf innerMapDesc)) {
            // Fallback: build on the fly (should not happen with properly cached descriptors)
            TopLevelFieldMap.FieldDesc.MapOf builtDesc =
                    DescriptorBuilder.buildMapDesc(group, batch.projectedSchema);
            return PqMapImpl.create(batch, builtDesc, -1, valueIdx);
        }
        return PqMapImpl.create(batch, innerMapDesc, -1, valueIdx);
    }

    private PqVariant createInnerVariant(int index) {
        TopLevelFieldMap.FieldDesc elementDesc = listDesc.elementDesc();
        if (!(elementDesc instanceof TopLevelFieldMap.FieldDesc.Variant variantDesc)) {
            throw new IllegalArgumentException("Element is not a variant");
        }
        if (variantDesc.root().typed() != null) {
            // Shredded variants in repeated contexts need position-aware
            // reassembly (tracked in hardwood#467).
            throw new UnsupportedOperationException(
                    "Shredded Variant inside a list element is not yet supported");
        }
        if (variantDesc.metadataCol() < 0) {
            throw new IllegalStateException(
                    "Variant list element requires its 'metadata' child in the projection");
        }
        int valueIdx = start + index;
        if (batch.isElementNull(variantDesc.metadataCol(), valueIdx)) {
            return null;
        }
        byte[] metadataBytes = ((BinaryBatchValues) batch.valueArrays[variantDesc.metadataCol()]).byteArrayAt(valueIdx);
        int valueCol = variantDesc.valueCol();
        if (valueCol < 0) {
            throw new IllegalStateException(
                    "Variant list element requires its 'value' child in the projection");
        }
        byte[] value = ((BinaryBatchValues) batch.valueArrays[valueCol]).byteArrayAt(valueIdx);
        return new PqVariantImpl(metadataBytes, value);
    }

    private boolean isStructElementNull(TopLevelFieldMap.FieldDesc.Struct structDesc, int valueIdx) {
        int primCol = structDesc.firstPrimitiveCol();
        if (primCol >= 0) {
            int defLevel = batch.getDefLevel(primCol, valueIdx);
            return defLevel < structDesc.schema().maxDefinitionLevel();
        }
        // Struct has no direct primitive child; fall back to the first leaf at any
        // depth. In leaf mode (subLevel < 0) `valueIdx` is already a leaf position
        // in that leaf column. In nested mode it is a rep-level ordinal at the
        // struct's level; chase through the leaf column's multi-level offsets.
        int leafCol = structDesc.firstLeafProjCol();
        if (leafCol < 0) {
            return false;
        }
        int pos = valueIdx;
        if (subLevel >= 0) {
            int structRep = structDesc.schema().maxRepetitionLevel();
            int leafMaxRep = batch.getMaxRepLevel(leafCol);
            for (int k = structRep; k < leafMaxRep; k++) {
                pos = batch.getLevelStart(leafCol, k, pos);
            }
        }
        int defLevel = batch.getDefLevel(leafCol, pos);
        return defLevel < structDesc.schema().maxDefinitionLevel();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        int n = size();
        for (int i = 0; i < n; i++) {
            if (i > 0) sb.append(", ");
            FlyweightFormatter.appendValue(sb, get(i));
        }
        sb.append(']');
        return sb.toString();
    }

    // ==================== Internal: Bounds Check ====================

    private void checkBounds(int index) {
        if (index < 0 || index >= size()) {
            throw new IndexOutOfBoundsException("Index " + index + " out of range [0, " + size() + ")");
        }
    }

    // ==================== Internal: Lazy List Views ====================

    /// Lazy [List] view over the list's leaf-column values. `size()` is the
    /// list length; `get(int)` decodes one element on demand via `converter`,
    /// short-circuiting nulls before the converter runs.
    private final class LeafList<T> extends AbstractList<T> {
        private final Function<Object, T> converter;

        LeafList(Function<Object, T> converter) {
            this.converter = converter;
        }

        @Override
        public int size() {
            return end - start;
        }

        @Override
        public T get(int index) {
            Objects.checkIndex(index, size());
            int projCol = listDesc.firstLeafProjCol();
            int pos = start + index;
            if (batch.isElementNull(projCol, pos)) {
                return null;
            }
            return converter.apply(batch.getValue(projCol, pos));
        }
    }

    /// Lazy [List] view over nested elements (structs / inner lists / maps /
    /// variants). `get(int)` defers to a per-element flyweight factory.
    private final class NestedList<T> extends AbstractList<T> {
        private final IntFunction<T> creator;

        NestedList(IntFunction<T> creator) {
            this.creator = creator;
        }

        @Override
        public int size() {
            return end - start;
        }

        @Override
        public T get(int index) {
            Objects.checkIndex(index, size());
            return creator.apply(index);
        }
    }
}
