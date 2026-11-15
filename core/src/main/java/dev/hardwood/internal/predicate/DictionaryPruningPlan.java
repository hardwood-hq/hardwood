/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.BitSet;
import java.util.function.Predicate;

import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.metadata.RowGroup;

/// The plan of a row group's dictionary pruning: the columns whose dictionary it reads. A
/// [DictionarySource] and [BloomFilterSource] that read nothing: it proves no literal absent and
/// records which columns' dictionaries the evaluator asked for, leaving out a column whose Bloom
/// filter the same leaf asked for first.
///
/// Evaluating a row group with it through [RowGroupFilterEvaluator#planDictionaryPruning]
/// yields the columns whose dictionary a leaf may consult: since no dictionary decides a leaf
/// here, no `AND` or `OR` stops short of one a real evaluation could reach. A leaf asks its
/// column's Bloom filter before its dictionary, so a dictionary asked for right after the same
/// column's Bloom filter, in a chunk that carries one, is the one that filter answers first, and is
/// left out.
public final class DictionaryPruningPlan implements DictionarySource, BloomFilterSource {

    private final RowGroup rowGroup;
    private final BitSet columns = new BitSet();
    /// the column whose Bloom filter the current leaf asked for, in a chunk that carries one; `-1`
    /// for none
    private int bloomed = -1;

    /// A plan for the columns of `rowGroup`.
    public DictionaryPruningPlan(RowGroup rowGroup) {
        this.rowGroup = rowGroup;
    }

    /// The evaluator moves on to the next leaf.
    void nextLeaf() {
        bloomed = -1;
    }

    /// Notes that the current leaf asked for `columnIndex`'s Bloom filter, where its chunk declares
    /// one the read uses ([RowGroupBloomFilterSource#declaresFilter]), and returns `null`, so that
    /// the leaf goes on to its dictionary as for a filter that cannot prove the literal absent.
    @Override
    public BloomFilter forColumn(int columnIndex) {
        if (columnIndex >= 0 && columnIndex < rowGroup.columns().size()
                && RowGroupBloomFilterSource.declaresFilter(rowGroup.columns().get(columnIndex).metaData())) {
            bloomed = columnIndex;
        }
        return null;
    }

    /// Records `columnIndex`, unless the current leaf asked for its Bloom filter, and proves nothing
    /// absent.
    @Override
    public boolean absent(int columnIndex, Predicate<Dictionary> check) {
        if (columnIndex >= 0 && columnIndex != bloomed) {
            columns.set(columnIndex);
        }
        bloomed = -1;
        return false;
    }

    /// The file ordinals of the columns asked for.
    public BitSet columns() {
        return (BitSet) columns.clone();
    }
}
