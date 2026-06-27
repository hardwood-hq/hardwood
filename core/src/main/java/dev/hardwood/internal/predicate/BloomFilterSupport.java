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
    static boolean absent(BloomFilterSource bloomFilters, int columnIndex, int value) {
        BloomFilter bloomFilter = filterFor(bloomFilters, columnIndex);
        return bloomFilter != null && !bloomFilter.mightContain(XxHash64.hash(value));
    }

    /// Single-value bloom check for `INT64` values; see the `INT32` overload.
    static boolean absent(BloomFilterSource bloomFilters, int columnIndex, long value) {
        BloomFilter bloomFilter = filterFor(bloomFilters, columnIndex);
        return bloomFilter != null && !bloomFilter.mightContain(XxHash64.hash(value));
    }

    /// Single-value bloom check for binary values; see the `INT32` overload.
    static boolean absent(BloomFilterSource bloomFilters, int columnIndex, byte[] value) {
        BloomFilter bloomFilter = filterFor(bloomFilters, columnIndex);
        return bloomFilter != null && !bloomFilter.mightContain(XxHash64.hash(value));
    }

    /// Single-value bloom check for `FLOAT` values; see the `INT32` overload.
    ///
    /// Bloom filters hash the raw IEEE-754 bits, which distinguish `-0.0f` from `+0.0f` and every
    /// NaN bit pattern — but `FLOAT` equality treats `-0.0f == +0.0f` and `NaN != NaN`. Probing
    /// those by raw bits could prove a value absent that an equal stored value would match, so they
    /// are never pruned here (statistics still apply).
    static boolean absent(BloomFilterSource bloomFilters, int columnIndex, float value) {
        if (value == 0.0f || Float.isNaN(value)) {
            return false;
        }
        BloomFilter bloomFilter = filterFor(bloomFilters, columnIndex);
        return bloomFilter != null && !bloomFilter.mightContain(XxHash64.hash(value));
    }

    /// Single-value bloom check for `DOUBLE` values. See the `FLOAT` overload for the `±0` / `NaN`
    /// carve-out.
    static boolean absent(BloomFilterSource bloomFilters, int columnIndex, double value) {
        if (value == 0.0 || Double.isNaN(value)) {
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

    /// Resolves the column's bloom filter, or `null` when no source is supplied or the column
    /// carries no filter. Looking the filter up before hashing lets the callers above skip the
    /// probe-value hash entirely on the statistics-only path, where no source is present.
    private static BloomFilter filterFor(BloomFilterSource bloomFilters, int columnIndex) {
        return bloomFilters == null ? null : bloomFilters.forColumn(columnIndex);
    }
}
