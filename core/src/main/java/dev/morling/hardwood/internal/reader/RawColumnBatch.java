/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import dev.morling.hardwood.schema.ColumnSchema;

/**
 * A batch of raw values with definition and repetition levels from a repeated column.
 * Used for list-of-struct assembly where values must be correlated across multiple columns.
 *
 * <p>Unlike {@link SimpleColumnBatch}, values are not pre-assembled into lists.
 * Instead, the raw values and their levels are preserved for multi-column correlation
 * in {@link RecordAssembler}.</p>
 */
public final class RawColumnBatch implements ColumnBatch {

    private final Object[] rawValues;
    private final int[] definitionLevels;
    private final int[] repetitionLevels;
    private final int recordCount;
    private final ColumnSchema column;

    public RawColumnBatch(Object[] rawValues, int[] definitionLevels, int[] repetitionLevels,
                          int recordCount, ColumnSchema column) {
        this.rawValues = rawValues;
        this.definitionLevels = definitionLevels;
        this.repetitionLevels = repetitionLevels;
        this.recordCount = recordCount;
        this.column = column;
    }

    @Override
    public int size() {
        return recordCount;
    }

    @Override
    public ColumnSchema getColumn() {
        return column;
    }

    /**
     * Number of raw values in this batch (may be greater than record count for repeated columns).
     */
    public int getRawValueCount() {
        return rawValues.length;
    }

    /**
     * Get the raw value at the given index.
     */
    public Object getRawValue(int index) {
        return rawValues[index];
    }

    /**
     * Get the definition level at the given raw value index.
     */
    public int getDefinitionLevel(int index) {
        return definitionLevels[index];
    }

    /**
     * Get the repetition level at the given raw value index.
     */
    public int getRepetitionLevel(int index) {
        return repetitionLevels[index];
    }
}
