/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.Statistics;

/// Abstraction over min/max statistics, used by [RowGroupFilterEvaluator] to evaluate
/// predicates against either row-group-level [Statistics] or page-level [ColumnIndex] entries.
interface MinMaxStats {

    /// @return the minimum value as raw bytes, or `null` if absent
    byte[] minValue();

    /// @return the maximum value as raw bytes, or `null` if absent
    byte[] maxValue();

    /// Wraps a [Statistics] record.
    static MinMaxStats of(Statistics stats) {
        return new MinMaxStats() {
            @Override
            public byte[] minValue() {
                return stats.minValue();
            }

            @Override
            public byte[] maxValue() {
                return stats.maxValue();
            }
        };
    }

    /// Wraps a [ColumnIndex] entry for a specific page.
    static MinMaxStats ofPage(ColumnIndex columnIndex, int pageIndex) {
        return new MinMaxStats() {
            @Override
            public byte[] minValue() {
                return columnIndex.minValues().get(pageIndex);
            }

            @Override
            public byte[] maxValue() {
                return columnIndex.maxValues().get(pageIndex);
            }
        };
    }
}
