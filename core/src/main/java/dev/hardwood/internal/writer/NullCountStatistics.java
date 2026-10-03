/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import dev.hardwood.metadata.Statistics;

/// Counts nulls and nothing else, for a binary column whose sort order parquet-format leaves
/// undefined — `INTERVAL`, `UNKNOWN`, `GEOMETRY`, `GEOGRAPHY`. Such a column writes its null
/// count alone, so accumulating bounds would only produce values [ColumnChunkBuffer] discards.
/// See [StatisticsOrder].
final class NullCountStatistics extends BinaryStatistics {

    @Override
    void accept(byte[] array, int offset, int length) {
        // No ordering to extend bounds in.
    }

    @Override
    void mergeValues(BinaryStatistics page) {
        // No bounds to merge; the null count is merged by the caller.
    }

    @Override
    boolean hasValues() {
        return false;
    }

    @Override
    Statistics toStatistics() {
        return new Statistics(null, null, nullCount, null, false);
    }

    @Override
    byte[] indexMin() {
        throw new UnsupportedOperationException("A column without an order has no bounds");
    }

    @Override
    byte[] indexMax() {
        throw new UnsupportedOperationException("A column without an order has no bounds");
    }

    @Override
    int compareBounds(byte[] left, byte[] right) {
        throw new UnsupportedOperationException("A column without an order has no bounds");
    }
}
