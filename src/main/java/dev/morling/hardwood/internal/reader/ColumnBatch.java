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
 * Holds a batch of values read from a single column.
 * Used internally by RowReader for batched parallel column fetching.
 *
 * @see SimpleColumnBatch for pre-assembled values (flat columns, simple lists)
 * @see RawColumnBatch for raw values with levels (list-of-struct assembly)
 */
public sealed

interface ColumnBatch
permits SimpleColumnBatch, RawColumnBatch
{

    /**
     * Number of records in this batch.
     */
    int size();

    /**
     * The column this batch belongs to.
     */
    ColumnSchema getColumn();

    /**
     * A value along with its definition and repetition levels.
     * Used for multi-column list-of-struct assembly.
     */
    record ValueWithLevels(Object value, int defLevel, int repLevel) {
    }
}
