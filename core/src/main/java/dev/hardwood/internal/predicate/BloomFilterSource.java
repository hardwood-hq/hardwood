/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.bloomfilter.BloomFilter;

/// Column-indexed access to a row group's bloom filters.
///
/// Decouples [RowGroupFilterEvaluator] from the I/O needed to fetch a filter, so the evaluator
/// stays unit-testable and pays no I/O when no eligible predicate consults a filter.
public interface BloomFilterSource {

    /// Returns the bloom filter for the given original column index, or `null` if the column chunk
    /// carries none. Implementations may read lazily and cache the result (including absence).
    BloomFilter forColumn(int columnIndex);
}
