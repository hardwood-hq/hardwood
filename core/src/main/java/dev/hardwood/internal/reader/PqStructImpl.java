/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.internal.variant.PqVariantImpl;
import dev.hardwood.internal.variant.VariantMetadata;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;

/// Flyweight [PqStruct] that navigates directly over column arrays.
///
/// Supports two modes:
///
/// - **Record mode**: resolves value position via `getValueIndex(projCol, rowIndex)`.
///       Used for top-level structs.</li>
/// - **Position mode**: uses a fixed value index directly.
///       Used for struct elements within lists/maps.</li>
final class PqStructImpl extends AbstractPqStruct {

    private final NestedBatchIndex batch;
    private final int rowIndex;     // >= 0 for record mode
    private final int valueIndex;   // >= 0 for position mode, -1 for record mode

    /// Record mode: value index resolved from batch offsets.
    PqStructImpl(NestedBatchIndex batch, TopLevelFieldMap.FieldDesc.Struct desc, int rowIndex) {
        super(desc);
        this.batch = batch;
        this.rowIndex = rowIndex;
        this.valueIndex = -1;
    }

    /// Position mode: fixed value index (for struct elements within lists).
    static PqStructImpl atPosition(NestedBatchIndex batch,
                                       TopLevelFieldMap.FieldDesc.Struct desc, int valueIndex) {
        return new PqStructImpl(batch, desc, -1, valueIndex);
    }

    private PqStructImpl(NestedBatchIndex batch, TopLevelFieldMap.FieldDesc.Struct desc,
                             int rowIndex, int valueIndex) {
        super(desc);
        this.batch = batch;
        this.rowIndex = rowIndex;
        this.valueIndex = valueIndex;
    }

    @Override
    protected boolean isElementNull(int projCol, int idx) {
        return batch.isElementNull(projCol, idx);
    }

    @Override
    protected Object valueArrays(int projCol) {
        return batch.valueArrays[projCol];
    }

    @Override
    protected int resolveValueIndex(int projCol) {
        return valueIndex >= 0 ? valueIndex : batch.getValueIndex(projCol, rowIndex);
    }

    @Override
    protected PqStruct readStruct(TopLevelFieldMap.FieldDesc.Struct structDesc) {
        if (isStructNull(structDesc)) {
            return null;
        }
        if (valueIndex >= 0) {
            return PqStructImpl.atPosition(batch, structDesc, valueIndex);
        }
        return new PqStructImpl(batch, structDesc, rowIndex);
    }

    @Override
    protected boolean isFieldNull(TopLevelFieldMap.FieldDesc child) {
        return switch (child) {
            case TopLevelFieldMap.FieldDesc.Primitive p -> {
                int idx = resolveValueIndex(p.projectedCol());
                yield batch.isElementNull(p.projectedCol(), idx);
            }
            case TopLevelFieldMap.FieldDesc.Struct s -> isStructNull(s);
            case TopLevelFieldMap.FieldDesc.ListOf l ->
                    PqListImpl.isListNull(batch, l, rowIndex, valueIndex);
            case TopLevelFieldMap.FieldDesc.MapOf m ->
                    PqMapImpl.isMapNull(batch, m, rowIndex, valueIndex);
            case TopLevelFieldMap.FieldDesc.Variant v -> isVariantNull(v);
        };
    }

    @Override
    protected Object readValueImpl(TopLevelFieldMap.FieldDesc child, boolean decode) {
        return switch (child) {
            case TopLevelFieldMap.FieldDesc.Primitive p -> {
                int idx = resolveValueIndex(p.projectedCol());
                Object raw = readRaw(p, idx);
                if (raw == null) yield null;
                yield decode ? ValueConverter.convertValue(raw, p.schema()) : raw;
            }
            case TopLevelFieldMap.FieldDesc.Struct s -> {
                if (isStructNull(s)) {
                    yield null;
                }
                yield valueIndex >= 0
                        ? PqStructImpl.atPosition(batch, s, valueIndex)
                        : new PqStructImpl(batch, s, rowIndex);
            }
            case TopLevelFieldMap.FieldDesc.ListOf l ->
                    PqListImpl.createGenericList(batch, l, rowIndex, valueIndex);
            case TopLevelFieldMap.FieldDesc.MapOf m ->
                    PqMapImpl.create(batch, m, rowIndex, valueIndex);
            // Variants are self-describing; there's no raw-vs-decoded split.
            case TopLevelFieldMap.FieldDesc.Variant v -> readVariant(v, v.schema().name());
        };
    }

    @Override
    public PqList getList(String name) {
        return PqListImpl.createGenericList(batch, listAt(lookupChild(name), name), rowIndex, valueIndex);
    }

    @Override
    public PqList getList(int fieldIndex) {
        TopLevelFieldMap.FieldDesc child = desc.children()[fieldIndex];
        return PqListImpl.createGenericList(batch, listAt(child, child.name()), rowIndex, valueIndex);
    }

    @Override
    public PqMap getMap(String name) {
        return PqMapImpl.create(batch, mapAt(lookupChild(name), name), rowIndex, valueIndex);
    }

    @Override
    public PqMap getMap(int fieldIndex) {
        TopLevelFieldMap.FieldDesc child = desc.children()[fieldIndex];
        return PqMapImpl.create(batch, mapAt(child, child.name()), rowIndex, valueIndex);
    }

    @Override
    public PqVariant getVariant(String name) {
        return readVariant(variantAt(lookupChild(name), name), name);
    }

    @Override
    public PqVariant getVariant(int fieldIndex) {
        TopLevelFieldMap.FieldDesc child = desc.children()[fieldIndex];
        return readVariant(variantAt(child, child.name()), child.name());
    }

    // ==================== Primitive Read Helpers ====================

    private PqVariant readVariant(TopLevelFieldMap.FieldDesc.Variant variantDesc, String fieldName) {
        if (variantDesc.metadataCol() < 0) {
            throw new IllegalStateException(
                    "Variant column '" + fieldName + "' requires its 'metadata' child in the projection");
        }
        int metaIdx = resolveValueIndex(variantDesc.metadataCol());
        if (batch.isElementNull(variantDesc.metadataCol(), metaIdx)) {
            return null;
        }
        byte[] metadataBytes = batch.getBinary(variantDesc.metadataCol(), metaIdx);

        if (variantDesc.root().typed() != null) {
            // Position-mode (struct inside a list/map) would need list-aware
            // indices inside the reassembler; record-mode uses the row index
            // directly. No fixture exercises the former today, and silently
            // returning bytes reassembled against row 0 would corrupt results.
            // Fail fast until list-aware reassembly is implemented.
            if (rowIndex < 0) {
                throw new UnsupportedOperationException(
                        "Shredded Variant inside a repeated context (list/map element) "
                                + "is not yet supported; field '" + fieldName + "'");
            }
            VariantMetadata meta = new VariantMetadata(metadataBytes);
            VariantShredReassembler reassembler = new VariantShredReassembler();
            reassembler.setCurrentMetadata(meta);
            byte[] value = reassembler.reassemble(variantDesc.root(), batch, rowIndex);
            if (value == null) {
                return null;
            }
            return new PqVariantImpl(meta, value, 0);
        }

        int valueCol = variantDesc.valueCol();
        if (valueCol < 0) {
            throw new IllegalStateException(
                    "Variant column '" + fieldName + "' requires its 'value' child in the projection");
        }
        int valIdx = resolveValueIndex(valueCol);
        byte[] value = batch.getBinary(valueCol, valIdx);
        return new PqVariantImpl(metadataBytes, value);
    }

    // ==================== Child Resolution ====================

    private static TopLevelFieldMap.FieldDesc.ListOf listAt(
            TopLevelFieldMap.FieldDesc child, String fieldName) {
        if (!(child instanceof TopLevelFieldMap.FieldDesc.ListOf listDesc)) {
            throw new IllegalArgumentException("Field '" + fieldName + "' is not a list");
        }
        return listDesc;
    }

    private static TopLevelFieldMap.FieldDesc.MapOf mapAt(
            TopLevelFieldMap.FieldDesc child, String fieldName) {
        if (!(child instanceof TopLevelFieldMap.FieldDesc.MapOf mapDesc)) {
            throw new IllegalArgumentException("Field '" + fieldName + "' is not a map");
        }
        return mapDesc;
    }

    private static TopLevelFieldMap.FieldDesc.Variant variantAt(
            TopLevelFieldMap.FieldDesc child, String fieldName) {
        if (!(child instanceof TopLevelFieldMap.FieldDesc.Variant variantDesc)) {
            throw new IllegalArgumentException("Field '" + fieldName + "' is not annotated as VARIANT");
        }
        return variantDesc;
    }

    // ==================== Null Checks ====================

    private boolean isVariantNull(TopLevelFieldMap.FieldDesc.Variant desc) {
        int col = desc.metadataCol() >= 0 ? desc.metadataCol() : desc.valueCol();
        if (col < 0) {
            return true;
        }
        int idx = resolveValueIndex(col);
        int defLevel = batch.getDefLevel(col, idx);
        return defLevel < desc.nullDefLevel();
    }

    private boolean isStructNull(TopLevelFieldMap.FieldDesc.Struct structDesc) {
        int primCol = structDesc.firstPrimitiveCol();
        if (primCol >= 0) {
            int idx = resolveValueIndex(primCol);
            int defLevel = batch.getDefLevel(primCol, idx);
            return defLevel < structDesc.schema().maxDefinitionLevel();
        }
        // Struct has no direct primitive child; fall back to the first leaf at any
        // depth. In record-index mode the recorded value index already points to
        // the leaf position. In position mode, `valueIndex` is a rep-level ordinal
        // at the struct's level, so chase through the leaf column's multi-level
        // offsets to reach the leaf position for this struct instance.
        int leafCol = structDesc.firstLeafProjCol();
        if (leafCol < 0) {
            return false;
        }
        int pos;
        if (valueIndex >= 0) {
            pos = valueIndex;
            int structRep = structDesc.schema().maxRepetitionLevel();
            int leafMaxRep = batch.getMaxRepLevel(leafCol);
            for (int k = structRep; k < leafMaxRep; k++) {
                pos = batch.getLevelStart(leafCol, k, pos);
            }
        } else {
            pos = batch.getValueIndex(leafCol, rowIndex);
        }
        int defLevel = batch.getDefLevel(leafCol, pos);
        return defLevel < structDesc.schema().maxDefinitionLevel();
    }
}
