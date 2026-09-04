/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.internal.bloomfilter.XxHash64;

/// Shared utilities for evaluating equality / membership predicates against a row group's bloom
/// filters.
///
/// Used by [RowGroupFilterEvaluator] alongside [StatisticsFilterSupport]: statistics prove a value
/// out of range, while a bloom filter proves an in-range value definitely absent — either one is
/// sufficient to drop the row group. Every check resolves the column's filter before hashing, so
/// the statistics-only path (no [BloomFilterSource]) never pays for the probe hash.
final class BloomFilterSupport {

    private BloomFilterSupport() {
    }

    /// Whether the column's bloom filter proves the `INT32` `value` is absent. Returns `false`
    /// (cannot prove absence) when no source is supplied or the column carries no filter.
    static boolean valueAbsent(BloomFilterSource bloomFilters, int columnIndex, int value) {
        BloomFilter bloomFilter = filterFor(bloomFilters, columnIndex);
        return bloomFilter != null && !bloomFilter.mightContain(XxHash64.hash(value));
    }

    /// Single-value bloom check for `INT64` values; see the `INT32` overload.
    static boolean valueAbsent(BloomFilterSource bloomFilters, int columnIndex, long value) {
        BloomFilter bloomFilter = filterFor(bloomFilters, columnIndex);
        return bloomFilter != null && !bloomFilter.mightContain(XxHash64.hash(value));
    }

    /// Single-value bloom check for binary values; see the `INT32` overload.
    static boolean valueAbsent(BloomFilterSource bloomFilters, int columnIndex, byte[] value) {
        BloomFilter bloomFilter = filterFor(bloomFilters, columnIndex);
        return bloomFilter != null && !bloomFilter.mightContain(XxHash64.hash(value));
    }

    /// Single-value bloom check for `FLOAT` values; see the `INT32` overload.
    ///
    /// Bloom filters hash raw IEEE-754 bits and the record matcher uses [Float#compare(float, float)],
    /// so both distinguish `-0.0f` from `+0.0f` and signed zeros are safe to probe. NaN values are
    /// not: the matcher treats different NaN payloads as equal while raw-bit hashing distinguishes
    /// them, so a bloom miss cannot prove a NaN absent.
    static boolean valueAbsent(BloomFilterSource bloomFilters, int columnIndex, float value) {
        if (Float.isNaN(value)) {
            return false;
        }
        BloomFilter bloomFilter = filterFor(bloomFilters, columnIndex);
        return bloomFilter != null && !bloomFilter.mightContain(XxHash64.hash(value));
    }

    /// Single-value bloom check for `DOUBLE` values. See the `FLOAT` overload for signed-zero and
    /// NaN behavior.
    static boolean valueAbsent(BloomFilterSource bloomFilters, int columnIndex, double value) {
        if (Double.isNaN(value)) {
            return false;
        }
        BloomFilter bloomFilter = filterFor(bloomFilters, columnIndex);
        return bloomFilter != null && !bloomFilter.mightContain(XxHash64.hash(value));
    }

    /// Whether the column's bloom filter proves every listed `INT32` value is absent, so an `IN`
    /// list matches no rows. Returns `false` when no source is supplied or the column carries no
    /// filter — and as soon as any value might be present.
    static boolean absentAll(BloomFilterSource bloomFilters, int columnIndex, int[] values) {
        BloomFilter bloomFilter = filterFor(bloomFilters, columnIndex);
        if (bloomFilter == null) {
            return false;
        }
        for (int value : values) {
            if (bloomFilter.mightContain(XxHash64.hash(value))) {
                return false;
            }
        }
        return true;
    }

    /// `IN`-list bloom check for `INT64` values. See [#absentAll(BloomFilterSource, int, int[])].
    static boolean absentAll(BloomFilterSource bloomFilters, int columnIndex, long[] values) {
        BloomFilter bloomFilter = filterFor(bloomFilters, columnIndex);
        if (bloomFilter == null) {
            return false;
        }
        for (long value : values) {
            if (bloomFilter.mightContain(XxHash64.hash(value))) {
                return false;
            }
        }
        return true;
    }

    /// `IN`-list bloom check for binary values. See [#absentAll(BloomFilterSource, int, int[])].
    static boolean absentAll(BloomFilterSource bloomFilters, int columnIndex, byte[][] values) {
        BloomFilter bloomFilter = filterFor(bloomFilters, columnIndex);
        if (bloomFilter == null) {
            return false;
        }
        for (byte[] value : values) {
            if (bloomFilter.mightContain(XxHash64.hash(value))) {
                return false;
            }
        }
        return true;
    }

    /// `IN`-list bloom check for `double` values across DOUBLE and FLOAT columns.
    /// Returns `false` when any probe is NaN, as NaNs are not portably hashed into bloom filters.
    /// For FLOAT columns, non-representable probes are trivially absent, and representable probes
    /// are hashed at float width.
    static boolean absentAll(BloomFilterSource bloomFilters, int columnIndex, double[] values,
            boolean floatColumn) {
        for (double v : values) {
            if (Double.isNaN(v)) {
                return false;
            }
        }
        BloomFilter bloomFilter = filterFor(bloomFilters, columnIndex);
        if (bloomFilter == null) {
            return false;
        }
        if (floatColumn) {
            for (double v : values) {
                // Check if v is exactly float-representable (Double.isNaN handled above).
                // Note on -0.0: The sign bit survives the float round-trip and the hash,
                // so -0.0 is hashed at float width without special-casing.
                if ((double) (float) v == v) {
                    if (bloomFilter.mightContain(XxHash64.hash((float) v))) {
                        return false;
                    }
                }
                // Non-representable probes cannot match any stored 32-bit float and are trivially absent.
            }
            return true;
        }
        for (double value : values) {
            if (bloomFilter.mightContain(XxHash64.hash(value))) {
                return false;
            }
        }
        return true;
    }

    /// Resolves the column's bloom filter, or `null` when no source is supplied or the column
    /// carries no filter. Looking the filter up before hashing lets the callers above skip the
    /// probe-value hash entirely on the statistics-only path, where no source is present.
    private static BloomFilter filterFor(BloomFilterSource bloomFilters, int columnIndex) {
        return bloomFilters == null ? null : bloomFilters.forColumn(columnIndex);
    }
}
