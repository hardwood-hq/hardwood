/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.row;

import java.util.List;

import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// The top-level struct fields of a [FlatRowReader]'s projection.
///
/// No column of the projection repeats, so every leaf below a struct holds one value per row,
/// at the row's index in its batch. A struct is read through a [PqStructImpl] over a
/// [NestedBatchIndex] of the current batches, without offsets.
///
/// A flat batch reduces each leaf's definition levels to a present bit, which cannot tell a
/// null leaf from a null struct above it. For every struct that can be null, the leaf the
/// struct's nullness is read from (its first projected leaf, as [PqStructImpl] reads it) keeps
/// its definition levels in [BatchExchange.Batch#definitionLevels]. From them each batch
/// derives, for a top-level struct, a bitmap of the rows that hold it, which the reader keeps in
/// the struct field's validity slot.
///
/// A struct the reader returns can be held past `next()`, and reads its batch's arrays on every
/// call. A flat column refills the arrays of the holders it is handed back, so before the reader
/// hands back the batch of a leaf below a struct, [#detach] takes the batch's arrays off its
/// holder: the next fill allocates new ones, as a nested batch's does, and a held struct keeps
/// reading its own.
final class FlatStructs {

    /// The descriptor of each field that is a struct, by field position; `null` for a primitive.
    private final TopLevelFieldMap.FieldDesc.Struct[] byField;
    /// The column whose definition levels say whether each field's struct is present, by field
    /// position; `-1` for a primitive and for a struct no row can leave null.
    private final int[] presenceColumn;
    /// The scratch bitmap of each field's struct presence, by field position, reused per batch.
    private final long[][] presenceWords;
    /// Whether each payload column keeps its definition levels, by decoded index.
    private final boolean[] keepsLevels;
    /// Whether each payload column is a leaf below a struct, by decoded index.
    private final boolean[] belowStruct;
    private final ColumnSchema[] columnSchemas;
    private final ProjectedSchema payload;
    private NestedBatchIndex batch;

    private FlatStructs(TopLevelFieldMap.FieldDesc.Struct[] byField, int[] presenceColumn, boolean[] keepsLevels,
                        ProjectedSchema payload) {
        this.byField = byField;
        this.presenceColumn = presenceColumn;
        this.presenceWords = new long[byField.length][];
        this.keepsLevels = keepsLevels;
        this.columnSchemas = payload.getProjectedColumns().toArray(new ColumnSchema[0]);
        this.belowStruct = new boolean[columnSchemas.length];
        for (int column = 0; column < columnSchemas.length; column++) {
            belowStruct[column] = columnSchemas[column].fieldPath().elements().size() > 1;
        }
        this.payload = payload;
    }

    /// The struct fields of `payload`, or `null` when every top-level field it projects is a
    /// primitive.
    ///
    /// @throws IllegalStateException if a top-level group of `payload` is not a plain struct,
    ///         which no flat read projects
    static FlatStructs create(FileSchema schema, ProjectedSchema payload) {
        int[] fieldIndices = payload.getProjectedFieldIndices();
        List<SchemaNode> rootChildren = schema.getRootNode().children();
        TopLevelFieldMap fieldMap = null;
        TopLevelFieldMap.FieldDesc.Struct[] byField = new TopLevelFieldMap.FieldDesc.Struct[fieldIndices.length];
        int[] presenceColumn = new int[fieldIndices.length];
        boolean[] keepsLevels = new boolean[payload.getProjectedColumnCount()];
        for (int field = 0; field < fieldIndices.length; field++) {
            presenceColumn[field] = -1;
            if (rootChildren.get(fieldIndices[field]) instanceof SchemaNode.PrimitiveNode) {
                continue;
            }
            if (fieldMap == null) {
                fieldMap = TopLevelFieldMap.build(schema, payload);
            }
            if (!(fieldMap.getByOriginalIndex(fieldIndices[field]) instanceof TopLevelFieldMap.FieldDesc.Struct struct)) {
                throw new IllegalStateException("Field '" + rootChildren.get(fieldIndices[field]).name()
                        + "' is not a struct, which a flat read cannot hold");
            }
            byField[field] = struct;
            presenceColumn[field] = nullableWitness(struct);
            keepLevels(struct, keepsLevels);
        }
        return fieldMap == null ? null : new FlatStructs(byField, presenceColumn, keepsLevels, payload);
    }

    /// Whether the payload column at decoded index `column` keeps its definition levels.
    boolean keepsLevels(int column) {
        return column < keepsLevels.length && keepsLevels[column];
    }

    /// Takes the current batches of the payload columns, and writes the presence bitmap of each
    /// struct field into `validity` at its position, [FlatRowReader#ALL_PRESENT] for a struct no
    /// row leaves null.
    ///
    /// @param batches the current batch of each decoded column, the payload's first
    /// @param records the rows of the batches
    void load(BatchExchange.Batch[] batches, int records, String fileName, long[][] validity) {
        int columnCount = columnSchemas.length;
        Object[] values = new Object[columnCount];
        long[][] columnValidity = new long[columnCount][];
        int[][] levels = new int[columnCount][];
        for (int column = 0; column < columnCount; column++) {
            BatchExchange.Batch columnBatch = batches[column];
            values[column] = columnBatch.values;
            columnValidity[column] = columnBatch.validity;
            levels[column] = keepsLevels[column] ? columnBatch.definitionLevels : null;
        }
        batch = NestedBatchIndex.rowAligned(values, levels, columnValidity, columnSchemas, payload, fileName, records);
        for (int field = 0; field < byField.length; field++) {
            if (byField[field] != null) {
                int column = presenceColumn[field];
                validity[field] = column < 0
                        ? FlatRowReader.ALL_PRESENT
                        : presence(field, levels[column], byField[field].schema().maxDefinitionLevel(), records);
            }
        }
    }

    /// Takes the arrays off the current batches of the leaves below a struct, ahead of their
    /// holders going back to their exchanges, so that no later fill writes into arrays a held
    /// struct reads. The validity bitmap is the batch's own already.
    ///
    /// @param batches the current batch of each decoded column, the payload's first
    void detach(BatchExchange.Batch[] batches) {
        for (int column = 0; column < belowStruct.length; column++) {
            BatchExchange.Batch columnBatch = batches[column];
            if (belowStruct[column] && columnBatch != null) {
                columnBatch.values = null;
                if (columnBatch.definitionLevels != null) {
                    columnBatch.definitionLevels = new int[columnBatch.definitionLevels.length];
                }
            }
        }
    }

    /// The struct field at `field` in row `rowIndex` of the current batches, which the caller has
    /// established is present.
    PqStruct at(int field, int rowIndex) {
        return new PqStructImpl(batch, byField[field], rowIndex);
    }

    /// Sets a bit for every row whose definition level reaches the struct's own, in the field's
    /// scratch bitmap.
    private long[] presence(int field, int[] levels, int structLevel, int records) {
        int words = (records + 63) >>> 6;
        long[] bits = presenceWords[field];
        if (bits == null || bits.length < words) {
            bits = new long[words];
            presenceWords[field] = bits;
        }
        for (int word = 0; word < words; word++) {
            int base = word << 6;
            int end = Math.min(64, records - base);
            long bitsOfWord = 0L;
            for (int bit = 0; bit < end; bit++) {
                if (levels[base + bit] >= structLevel) {
                    bitsOfWord |= 1L << bit;
                }
            }
            bits[word] = bitsOfWord;
        }
        return bits;
    }

    /// Marks the leaf each struct at or below `struct` reads its nullness from, for every one of
    /// them that can be null.
    private static void keepLevels(TopLevelFieldMap.FieldDesc.Struct struct, boolean[] keepsLevels) {
        int witness = nullableWitness(struct);
        if (witness >= 0) {
            keepsLevels[witness] = true;
        }
        for (TopLevelFieldMap.FieldDesc child : struct.children()) {
            if (child instanceof TopLevelFieldMap.FieldDesc.Struct childStruct) {
                keepLevels(childStruct, keepsLevels);
            }
        }
    }

    /// The column whose definition levels say whether `struct` is present, as [PqStructImpl]
    /// chooses it, or `-1` when the struct and every group above it are required.
    private static int nullableWitness(TopLevelFieldMap.FieldDesc.Struct struct) {
        if (struct.schema().maxDefinitionLevel() == 0) {
            return -1;
        }
        return struct.firstPrimitiveCol() >= 0 ? struct.firstPrimitiveCol() : struct.firstLeafProjCol();
    }
}
