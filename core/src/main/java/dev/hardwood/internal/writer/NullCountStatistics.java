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
final class NullCountStatistics implements BinaryStatistics {

    private long nullCount;

    @Override
    public void accept(byte[] array, int offset, int length) {
        // No ordering to extend bounds in.
    }

    @Override
    public void acceptNull() {
        nullCount++;
    }

    @Override
    public Statistics toStatistics() {
        return new Statistics(null, null, nullCount, null, false);
    }
}
