/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import dev.hardwood.metadata.Statistics;

/// Accumulates the statistics of one data page or of one column chunk, in the column's order. A
/// chunk's collector is the merge of its pages' collectors, so each value is compared once, by
/// the page it lands on, and the chunk's bounds and counts follow from the pages'.
///
/// @param <C> the concrete collector, which merges only with its own kind
abstract class StatisticsCollector<C extends StatisticsCollector<C>> {

    /// The `NaN` count of a type that has no `NaN`.
    static final long NO_NAN_COUNT = -1;

    long nullCount;

    /// Counts `count` absent (null) slots.
    final void addNulls(long count) {
        nullCount += count;
    }

    /// Extends this collector by everything `page` collected.
    final void merge(C page) {
        nullCount += page.nullCount;
        mergeValues(page);
    }

    /// Extends the bounds and value counts by `page`'s.
    abstract void mergeValues(C page);

    /// Whether any value extended the bounds.
    abstract boolean hasValues();

    /// The accumulated statistics, as the column chunk metadata records them.
    abstract Statistics toStatistics();

    /// The lower bound as a `ColumnIndex` records it. Only called where [#hasValues()].
    abstract byte[] indexMin();

    /// The upper bound as a `ColumnIndex` records it. Only called where [#hasValues()].
    abstract byte[] indexMax();

    /// Compares two bounds as [#indexMin()] and [#indexMax()] encode them, in the column's order.
    abstract int compareBounds(byte[] left, byte[] right);

    /// How many present values were `NaN`, or [#NO_NAN_COUNT] for a type without `NaN`.
    long nanCount() {
        return NO_NAN_COUNT;
    }
}
