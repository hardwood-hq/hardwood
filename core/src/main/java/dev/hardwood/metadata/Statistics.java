/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// Column chunk statistics for min/max values, null count, and distinct count.
///
/// Used for predicate push-down: row groups whose statistics prove that no rows
/// can match a filter predicate are skipped entirely.
///
/// @param minValue minimum value encoded as raw bytes (little-endian), or `null` if absent
/// @param maxValue maximum value encoded as raw bytes (little-endian), or `null` if absent
/// @param nullCount number of null values in the column chunk, or `null` if absent
/// @param distinctCount number of distinct values in the column chunk, or `null` if absent
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public record Statistics(
        byte[] minValue,
        byte[] maxValue,
        Long nullCount,
        Long distinctCount,
        boolean isMinMaxDeprecated) {
}
