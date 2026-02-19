/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.schema.ColumnSchema;

/**
 * Base sealed interface for typed column data with primitive arrays.
 * <p>
 * Stores values in typed primitive arrays to eliminate boxing overhead.
 * Extended by {@link FlatColumnData} for flat schemas and {@link NestedColumnData}
 * for nested schemas.
 * </p>
 */
public sealed interface TypedColumnData permits FlatColumnData, NestedColumnData {

    ColumnSchema column();

    int recordCount();

    /** Total number of values (may differ from recordCount for nested schemas). */
    int valueCount();

    /** Get the value at index, boxing primitives. */
    Object getValue(int index);

    /** Check if the value at index is null. */
    boolean isNull(int index);
}
