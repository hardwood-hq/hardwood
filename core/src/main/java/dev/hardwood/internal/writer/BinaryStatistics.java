/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.schema.ColumnSchema;

/// A binary column chunk's statistics accumulator.
///
/// The bytes of a `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` column carry no single ordering: the
/// same physical type is compared unsigned byte-wise when it holds a string, as a signed
/// big-endian integer when it holds a decimal, as a signed little-endian integer when it holds a
/// timestamp, and as a represented floating-point value when it holds a half-precision float. The logical type picks the accumulator, so the ordering is
/// decided once per chunk rather than tested per value.
interface BinaryStatistics {

    /// Selects the accumulator for a binary column's sort order.
    ///
    /// @param column the column's schema, whose logical type defines the ordering
    /// @param truncationLength the maximum bound length, applied only where truncation preserves
    ///        the ordering
    static BinaryStatistics forColumn(ColumnSchema column, int truncationLength) {
        if (!StatisticsOrder.supportsBounds(column)) {
            // No ordering exists to accumulate in, and the bounds would be dropped at flush, so
            // the values are never inspected — a `GEOMETRY` chunk would otherwise copy a blob
            // out on every bound extension only to discard it.
            return new NullCountStatistics();
        }
        LogicalType logicalType = column.logicalType();
        if (logicalType instanceof LogicalType.Float16Type) {
            return new Float16StatisticsCollector();
        }
        if (logicalType instanceof LogicalType.DecimalType) {
            // A decimal's bounds compare as signed big-endian integers, under which a prefix is
            // not a lower bound, so they are never truncated.
            return new BinaryStatisticsCollector(BinaryStatisticsCollector.Order.SIGNED_BIG_ENDIAN,
                    Integer.MAX_VALUE);
        }
        if (logicalType instanceof LogicalType.TimestampType) {
            // Only a FIXED_LEN_BYTE_ARRAY(12) timestamp reaches a binary accumulator, and it is
            // already at its bound length.
            return new BinaryStatisticsCollector(BinaryStatisticsCollector.Order.SIGNED_LITTLE_ENDIAN,
                    Integer.MAX_VALUE);
        }
        // A FIXED_LEN_BYTE_ARRAY is already at its bound length, so only a BYTE_ARRAY truncates.
        return new BinaryStatisticsCollector(BinaryStatisticsCollector.Order.LEXICOGRAPHIC,
                column.typeLength() == null ? truncationLength : Integer.MAX_VALUE);
    }

    /// Extends the bounds with a present value.
    void accept(byte[] value);

    /// Counts an absent (null) slot.
    void acceptNull();

    /// The accumulated chunk statistics.
    Statistics toStatistics();
}
