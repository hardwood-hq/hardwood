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
/// sufficient to drop the row group.
///
/// Every method here is a pure test against a filter already in hand: the caller resolves it, so
/// the read stays in [RowGroupFilterEvaluator] where the guards that decide whether it is worth
/// doing already are.
final class BloomFilterSupport {

    private BloomFilterSupport() {
    }

    /// Whether the column's bloom filter proves the `INT32` `value` is absent. Returns `false`
    /// (cannot prove absence) when the column carries no filter.
    static boolean valueAbsent(BloomFilter bloomFilter, int value) {
        return bloomFilter != null && !bloomFilter.mightContain(XxHash64.hash(value));
    }

    /// Single-value bloom check for `INT64` values; see the `INT32` overload.
    static boolean valueAbsent(BloomFilter bloomFilter, long value) {
        return bloomFilter != null && !bloomFilter.mightContain(XxHash64.hash(value));
    }

    /// Single-value bloom check for binary values; see the `INT32` overload.
    static boolean valueAbsent(BloomFilter bloomFilter, byte[] value) {
        return bloomFilter != null && !bloomFilter.mightContain(XxHash64.hash(value));
    }

    /// Single-value bloom check for `FLOAT` values; see the `INT32` overload.
    ///
    /// Bloom filters hash raw IEEE-754 bits and the record matcher uses [Float#compare(float, float)],
    /// so both distinguish `-0.0f` from `+0.0f` and signed zeros are safe to probe. NaN values are
    /// not: the matcher treats different NaN payloads as equal while raw-bit hashing distinguishes
    /// them, so a bloom miss cannot prove a NaN absent.
    static boolean valueAbsent(BloomFilter bloomFilter, float value) {
        if (Float.isNaN(value)) {
            return false;
        }
        return bloomFilter != null && !bloomFilter.mightContain(XxHash64.hash(value));
    }

    /// Single-value bloom check for `DOUBLE` values. See the `FLOAT` overload for signed-zero and
    /// NaN behavior.
    static boolean valueAbsent(BloomFilter bloomFilter, double value) {
        if (Double.isNaN(value)) {
            return false;
        }
        return bloomFilter != null && !bloomFilter.mightContain(XxHash64.hash(value));
    }

    /// Whether the column's bloom filter proves every listed `INT32` value is absent, so an `IN`
    /// list matches no rows. Returns `false` when the column carries no filter,
    /// and as soon as any value might be present.
    static boolean absentAll(BloomFilter bloomFilter, int[] values) {
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

    /// `IN`-list bloom check for `INT64` values. See [#absentAll(BloomFilter, int[])].
    static boolean absentAll(BloomFilter bloomFilter, long[] values) {
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

    /// `IN`-list bloom check for binary values. See [#absentAll(BloomFilter, int[])].
    static boolean absentAll(BloomFilter bloomFilter, byte[][] values) {
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
    static boolean absentAll(BloomFilter bloomFilter, double[] values, boolean floatColumn) {
        if (anyNaN(values)) {
            return false;
        }
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

    /// Whether any probe in an `IN` list is `NaN`, which makes the bloom check undecidable for the
    /// whole list. Callers evaluating a row group check this before resolving the filter, so the
    /// filter is not read for a list it could not decide either way.
    static boolean anyNaN(double[] values) {
        for (double value : values) {
            if (Double.isNaN(value)) {
                return true;
            }
        }
        return false;
    }
}
