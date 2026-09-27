/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.BitSet;

import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.metadata.RowGroup;

/// A [BloomFilterSource] that reads nothing: it answers "no filter" for every column and records
/// which columns the evaluator asked for.
///
/// Planning a row group with it through [RowGroupFilterEvaluator#planRowGroup] yields the
/// decision of its statistics alone and the columns whose bloom filter a leaf would consult. A
/// leaf its statistics drop asks for no filter, so the recorded columns are those the read
/// may have to fetch a filter for once it reaches the row group.
public final class BloomFilterColumnRecorder implements BloomFilterSource {

    private final int columnCount;
    private final BitSet columns = new BitSet();

    public BloomFilterColumnRecorder(RowGroup rowGroup) {
        this.columnCount = rowGroup.columns().size();
    }

    /// Records `columnIndex` and returns `null`. An index past the row group's columns is not
    /// recorded, as [RowGroupBloomFilterSource#forColumn] reads no filter for it.
    @Override
    public BloomFilter forColumn(int columnIndex) {
        if (columnIndex >= 0 && columnIndex < columnCount) {
            columns.set(columnIndex);
        }
        return null;
    }

    /// The file ordinals of the columns asked for.
    public BitSet columns() {
        return (BitSet) columns.clone();
    }
}
