/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;

/// Flyweight [PqStruct] for non-repeated struct columns on the flat reader path.
///
/// Backed directly by the flat column arrays owned by [FlatRowReader].
/// Because [FlatRowReader] guarantees `maxRepetitionLevel == 0` for every
/// column, there are no record offsets or repetition levels to navigate:
/// `rowIndex` is always the direct index into every column's value array.
///
/// Null detection for optional structs uses definition levels rather than
/// the validity bitset, so that a null leaf inside a present struct is
/// correctly distinguished from a null struct.
///
/// Lists, maps, and variants are not supported on the flat path — any call
/// to those accessors throws [UnsupportedOperationException].
class FlatPqStructImpl extends AbstractPqStruct {

    private final Object[] valueArrays;
    private final long[][] flatValidity;
    private final int[][] flatDefLevels;
    private final int rowIndex;

    FlatPqStructImpl(Object[] valueArrays, long[][] flatValidity, int[][] flatDefLevels,
                     TopLevelFieldMap.FieldDesc.Struct desc, int rowIndex) {
        super(desc);
        this.valueArrays = valueArrays;
        this.flatValidity = flatValidity;
        this.flatDefLevels = flatDefLevels;
        this.rowIndex = rowIndex;
    }

    @Override
    protected boolean isElementNull(int projCol, int idx) {
        return (flatValidity[projCol][idx >>> 6] & (1L << (idx & 63))) == 0;
    }

    @Override
    protected Object valueArrays(int projCol) {
        return valueArrays[projCol];
    }

    @Override
    protected int resolveValueIndex(int projCol) {
        return rowIndex;
    }

    @Override
    protected PqStruct readStruct(TopLevelFieldMap.FieldDesc.Struct structDesc) {
        if (isStructNull(structDesc)) {
            return null;
        }

        return new FlatPqStructImpl(valueArrays, flatValidity, flatDefLevels, structDesc, rowIndex);
    }

    @Override
    protected boolean isFieldNull(TopLevelFieldMap.FieldDesc child) {
        return switch (child) {
            case TopLevelFieldMap.FieldDesc.Primitive p -> {
                yield isElementNull(p.projectedCol(), rowIndex);
            }
            case TopLevelFieldMap.FieldDesc.Struct s -> isStructNull(s);
            case TopLevelFieldMap.FieldDesc.ListOf l ->
                    throw nestedUnsupported();
            case TopLevelFieldMap.FieldDesc.MapOf m ->
                    throw nestedUnsupported();
            case TopLevelFieldMap.FieldDesc.Variant v -> throw nestedUnsupported();
        };
    }

    @Override
    protected Object readValueImpl(TopLevelFieldMap.FieldDesc child, boolean decode) {
        return switch (child) {
            case TopLevelFieldMap.FieldDesc.Primitive p -> {
                Object raw = readRaw(p, rowIndex);
                if (raw == null) yield null;
                yield decode ? ValueConverter.convertValue(raw, p.schema()) : raw;
            }
            case TopLevelFieldMap.FieldDesc.Struct s -> {
                if (isStructNull(s)) {
                    yield null;
                }
                yield new FlatPqStructImpl(valueArrays, flatValidity, flatDefLevels, s, rowIndex);
            }
            case TopLevelFieldMap.FieldDesc.ListOf l ->
                    throw nestedUnsupported();
            case TopLevelFieldMap.FieldDesc.MapOf m ->
                    throw nestedUnsupported();
            // Variants are self-describing; there's no raw-vs-decoded split.
            case TopLevelFieldMap.FieldDesc.Variant v -> throw nestedUnsupported();
        };
    }


    // ==================== Nested Types ====================

    @Override
    public PqList getList(String name) {
        throw nestedUnsupported();
    }

    @Override
    public PqList getList(int fieldIndex) {
        throw nestedUnsupported();
    }

    @Override
    public PqMap getMap(String name) {
        throw nestedUnsupported();
    }

    @Override
    public PqMap getMap(int fieldIndex) {
        throw nestedUnsupported();
    }

    @Override
    public PqVariant getVariant(String name) {
        throw nestedUnsupported();
    }

    @Override
    public PqVariant getVariant(int fieldIndex) {
        throw nestedUnsupported();
    }

    // ==================== Null Checks ====================

    private boolean isStructNull(TopLevelFieldMap.FieldDesc.Struct structDesc) {
        int checkCol = structDesc.firstPrimitiveCol() >= 0
                ? structDesc.firstPrimitiveCol()
                : structDesc.firstLeafProjCol();
        int defLevel = flatDefLevels[checkCol] != null ? flatDefLevels[checkCol][rowIndex] : Integer.MAX_VALUE;
        int structDefLevel = structDesc.schema().maxDefinitionLevel();

        return defLevel < structDefLevel;
    }

    private static UnsupportedOperationException nestedUnsupported() {
        return new UnsupportedOperationException("Nested type access not supported for flat schemas");
    }

}
