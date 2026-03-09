/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/**
 * Statistics for a column chunk or data page.
 *
 * <p>Min/max values are stored as raw bytes in the physical type's sort order.
 * Use {@link dev.hardwood.filter.StatisticsConverter} to interpret them as
 * typed values for predicate evaluation.</p>
 *
 * @param min minimum value as raw bytes (Parquet's min_value field, or legacy min), or {@code null} if absent
 * @param max maximum value as raw bytes (Parquet's max_value field, or legacy max), or {@code null} if absent
 * @param nullCount number of null values, or {@code -1} if not available
 * @param distinctCount number of distinct values, or {@code -1} if not available
 * @param isMinMaxDeprecated {@code true} if only legacy (pre-PARQUET-1025) min/max fields were present
 * @see <a href="https://parquet.apache.org/docs/file-format/metadata/#statistics">File Format – Statistics</a>
 * @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
 */
public record Statistics(
        byte[] min,
        byte[] max,
        long nullCount,
        long distinctCount,
        boolean isMinMaxDeprecated) {

    /**
     * Returns {@code true} if this statistics instance has usable min/max bounds.
     */
    public boolean hasMinMax() {
        return min != null && max != null;
    }
}
