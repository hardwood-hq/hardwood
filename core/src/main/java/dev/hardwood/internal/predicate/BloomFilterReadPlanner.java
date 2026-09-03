/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;

/// Plans which bloom filter reads a row-group pruning pass may perform, before any of them
/// happen, so the fetches can be coalesced and issued in parallel
/// (see `_designs/BLOOM_FILTER_IO_COALESCING.md`).
///
/// Eligibility is not re-implemented here: [#plan] runs
/// [RowGroupFilterEvaluator#decideRowGroup] with a recording [BloomFilterSource] that answers
/// "no filter" for every column and records which columns are asked for. A leaf's bloom check
/// is reached only when its statistics decision is not [FilterDecision#CANNOT_MATCH], and
/// bloom/dictionary results can only short-circuit evaluation *earlier*, never later — so the
/// recorded set is a superset of the columns the real evaluation consults, and the rules that
/// decide eligibility (EQ/IN, physical types, NaN, FLOAT16) are the evaluator's own.
///
/// The planner is pure metadata: no I/O, no threads. Fetching the planned candidates is the
/// caller's job.
public final class BloomFilterReadPlanner {

    /// One planned bloom read: column `columnIndex` of row group `rowGroupIndex` may consult its
    /// filter, which lives at `offset` — always positive; a null or non-positive footer offset is
    /// left to the lazy path's existing warning — with the footer-declared `length` — always
    /// positive when present; a zero footer length cannot hold a filter and is likewise left to
    /// the lazy path — or `null` when the writer omitted `bloom_filter_length` (the legacy
    /// probe path).
    public record BloomCandidate(int rowGroupIndex, RowGroup rowGroup, int columnIndex,
                                 long offset, Integer length) {

        public BloomCandidate {
            if (offset <= 0) {
                throw new IllegalArgumentException(
                        "bloom_filter_offset must be positive but was " + offset);
            }
            if (length != null && length <= 0) {
                throw new IllegalArgumentException(
                        "bloom_filter_length must be positive when present but was " + length);
            }
        }
    }

    /// The planned reads for one file, grouped per row group.
    public record BloomFilterReadPlan(List<BloomCandidate> candidates) {

        public static final BloomFilterReadPlan EMPTY =
                new BloomFilterReadPlan(List.of());

        public BloomFilterReadPlan {
            candidates = List.copyOf(candidates);
        }

        /// The candidates for one row group, in column order.
        List<BloomCandidate> candidatesFor(int rowGroupIndex) {
            return candidates.stream()
                    .filter(candidate -> candidate.rowGroupIndex() == rowGroupIndex)
                    .toList();
        }
    }

    private BloomFilterReadPlanner() {
    }

    /// Plans the bloom reads [RowGroupFilterEvaluator#decideRowGroup] may perform over the given
    /// row groups. Columns whose chunk carries no usable filter — no or non-positive
    /// `bloom_filter_offset`, a zero `bloom_filter_length` (an empty region cannot hold a
    /// filter; the lazy path fails on it with its usual error), or data in another file —
    /// produce no candidate and stay on the lazy read path, which handles them exactly as it
    /// does without planning.
    ///
    /// @param predicate the filter predicate with column indices resolved to this file's
    ///        ordinals (the value `FileColumnOrdinals.filter()` supplies), or `null` for no filter
    /// @param rowGroups every row group of the file, before pruning
    public static BloomFilterReadPlan plan(ResolvedPredicate predicate, List<RowGroup> rowGroups) {
        if (predicate == null) {
            return BloomFilterReadPlan.EMPTY;
        }
        List<BloomCandidate> candidates = new ArrayList<>();
        for (int rowGroupIndex = 0; rowGroupIndex < rowGroups.size(); rowGroupIndex++) {
            RowGroup rowGroup = rowGroups.get(rowGroupIndex);
            candidates.addAll(planRowGroup(rowGroupIndex, rowGroup, predicate));
        }
        return candidates.isEmpty()
                ? BloomFilterReadPlan.EMPTY
                : new BloomFilterReadPlan(candidates);
    }

    private static List<BloomCandidate> planRowGroup(int rowGroupIndex, RowGroup rowGroup,
                                                     ResolvedPredicate predicate) {
        RecordingSource recording = new RecordingSource(rowGroup);
        RowGroupFilterEvaluator.decideRowGroup(predicate, rowGroup, recording, null);
        List<BloomCandidate> candidates = new ArrayList<>();
        for (int columnIndex : recording.consulted) {
            ColumnChunk columnChunk = rowGroup.columns().get(columnIndex);
            ColumnMetaData metaData = columnChunk.metaData();
            Long offset = metaData.bloomFilterOffset();
            Integer length = metaData.bloomFilterLength();
            if (offset == null || offset <= 0) {
                continue;
            }
            if (length != null && length == 0) {
                continue;
            }
            if (!sameFile(columnChunk)) {
                continue;
            }
            candidates.add(new BloomCandidate(rowGroupIndex, rowGroup, columnIndex, offset, length));
        }
        return candidates;
    }

    /// Whether this chunk stores its data in the file being read. A chunk naming another file
    /// yields no candidate: the lazy path re-checks and raises the same exception, message and
    /// timing as it does without planning.
    private static boolean sameFile(ColumnChunk columnChunk) {
        try {
            columnChunk.requireSameFile();
            return true;
        }
        catch (IOException e) {
            return false;
        }
    }

    /// A [BloomFilterSource] that answers "no filter" everywhere and records which columns the
    /// evaluator asks for. Mirrors [RowGroupBloomFilterSource#forColumn]'s bounds guard — an
    /// index past this row group's column count is conservatively "no filter" — so the planner
    /// never indexes `rowGroup.columns()` out of bounds.
    private static final class RecordingSource implements BloomFilterSource {

        private final int columnCount;
        private final Set<Integer> consulted = new LinkedHashSet<>();

        RecordingSource(RowGroup rowGroup) {
            this.columnCount = rowGroup.columns().size();
        }

        @Override
        public BloomFilter forColumn(int columnIndex) {
            if (columnIndex >= 0 && columnIndex < columnCount) {
                consulted.add(columnIndex);
            }
            return null;
        }
    }
}
