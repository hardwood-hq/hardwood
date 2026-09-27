/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.predicate.RowGroupBloomFilterSource;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;

/// The page-index and bloom-filter slices of a run of consecutive work items of one file, fetched
/// together: the ColumnIndex slices of all members merged into one [CoalescedRanges], the
/// OffsetIndex slices into another, the bloom-filter slices into a third.
///
/// Formed while the file is planned. The bloom filters are fetched by the first
/// [#bloomFiltersFor] call for any member that has one, the page index by the first
/// [#buffersFor] call for any member, whether from a column's retriever or from the prefetch of
/// the next row group. The window drops its fetched bytes once every member has been released,
/// or when the read closes.
final class IndexWindow {

    private final InputFile inputFile;
    private final int firstWorkItemIndex;
    private final RowGroup[] rowGroups;
    private final BitSet[] offsetIndexColumns;
    private final BitSet[] columnIndexColumns;
    private final BitSet[] bloomFilterColumns;
    private final String rowGroupLabel;
    private final boolean[] released;
    private int unreleased;
    private CoalescedRanges offsetIndexes;
    private CoalescedRanges columnIndexes;
    private CoalescedRanges bloomFilters;
    private RowGroupIndexBuffers[] buffers;

    private IndexWindow(InputFile inputFile, int firstWorkItemIndex, RowGroup[] rowGroups,
            BitSet[] offsetIndexColumns, BitSet[] columnIndexColumns, BitSet[] bloomFilterColumns,
            String rowGroupLabel) {
        this.inputFile = inputFile;
        this.firstWorkItemIndex = firstWorkItemIndex;
        this.rowGroups = rowGroups;
        this.offsetIndexColumns = offsetIndexColumns;
        this.columnIndexColumns = columnIndexColumns;
        this.bloomFilterColumns = bloomFilterColumns;
        this.rowGroupLabel = rowGroupLabel;
        this.released = new boolean[rowGroups.length];
        this.unreleased = rowGroups.length;

        CoalescedRanges.Builder offsetIndexBuilder = CoalescedRanges.builder();
        CoalescedRanges.Builder columnIndexBuilder = CoalescedRanges.builder();
        CoalescedRanges.Builder bloomFilterBuilder = CoalescedRanges.builder();
        for (int m = 0; m < rowGroups.length; m++) {
            RowGroupIndexBuffers.addRanges(rowGroups[m], offsetIndexColumns[m], columnIndexColumns[m],
                    offsetIndexBuilder, columnIndexBuilder);
            addBloomFilterRanges(rowGroups[m], bloomFilterColumns[m], bloomFilterBuilder);
        }
        this.offsetIndexes = offsetIndexBuilder.build();
        this.columnIndexes = columnIndexBuilder.build();
        this.bloomFilters = bloomFilterBuilder.build();
    }

    /// Splits one file's work items into consecutive windows. Each window takes as many of the
    /// following work items as fit its fetched bytes within `budgetBytes`, and at least one, so a
    /// file whose page index fits the budget is one window.
    ///
    /// @param budgetBytes the most bytes one window fetches, gaps included, unless a single row
    ///        group needs more
    /// @param firstWorkItemIndex the work-list index of the first work item
    /// @param rowGroups the work items' row groups, in work-list order
    /// @param rowGroupIndexes their row-group indexes in the file, for the fetch reason
    /// @param offsetIndexColumns per work item, the columns whose OffsetIndex it needs
    /// @param columnIndexColumns per work item, the columns whose ColumnIndex it needs
    /// @param bloomFilterColumns per work item, the columns whose bloom filter it may probe
    static List<IndexWindow> plan(long budgetBytes, InputFile inputFile, int firstWorkItemIndex,
            RowGroup[] rowGroups, int[] rowGroupIndexes, BitSet[] offsetIndexColumns,
            BitSet[] columnIndexColumns, BitSet[] bloomFilterColumns) {
        List<IndexWindow> windows = new ArrayList<>();
        int from = 0;
        while (from < rowGroups.length) {
            IndexWindow window = largest(budgetBytes, inputFile, firstWorkItemIndex, rowGroups,
                    rowGroupIndexes, offsetIndexColumns, columnIndexColumns, bloomFilterColumns,
                    from);
            windows.add(window);
            from += window.memberCount();
        }
        return windows;
    }

    /// The window over the most work items from `from` on that fits the budget, and at least one.
    /// Fetched bytes grow with every member added, so the count is found by galloping: counts of
    /// 1, 2, 4, … until one exceeds the budget or reaches the file's end, then bisection between
    /// the last that fit and the first that did not. Every candidate is at most twice the size of
    /// the window it finds, so planning a file costs time linear in its row groups.
    private static IndexWindow largest(long budgetBytes, InputFile inputFile,
            int firstWorkItemIndex, RowGroup[] rowGroups, int[] rowGroupIndexes,
            BitSet[] offsetIndexColumns, BitSet[] columnIndexColumns, BitSet[] bloomFilterColumns,
            int from) {
        int remaining = rowGroups.length - from;
        IndexWindow best = window(inputFile, firstWorkItemIndex, rowGroups, rowGroupIndexes,
                offsetIndexColumns, columnIndexColumns, bloomFilterColumns, from, 1);
        int fits = 1;
        int exceeds = remaining + 1;
        while (fits < remaining) {
            int count = (int) Math.min((long) fits * 2, remaining);
            IndexWindow candidate = window(inputFile, firstWorkItemIndex, rowGroups,
                    rowGroupIndexes, offsetIndexColumns, columnIndexColumns, bloomFilterColumns,
                    from, count);
            if (candidate.fetchedBytes() > budgetBytes) {
                exceeds = count;
                break;
            }
            best = candidate;
            fits = count;
        }
        while (exceeds - fits > 1) {
            int mid = (fits + exceeds) >>> 1;
            IndexWindow candidate = window(inputFile, firstWorkItemIndex, rowGroups,
                    rowGroupIndexes, offsetIndexColumns, columnIndexColumns, bloomFilterColumns,
                    from, mid);
            if (candidate.fetchedBytes() <= budgetBytes) {
                best = candidate;
                fits = mid;
            }
            else {
                exceeds = mid;
            }
        }
        return best;
    }

    private static IndexWindow window(InputFile inputFile, int firstWorkItemIndex,
            RowGroup[] rowGroups, int[] rowGroupIndexes, BitSet[] offsetIndexColumns,
            BitSet[] columnIndexColumns, BitSet[] bloomFilterColumns, int from, int count) {
        int first = rowGroupIndexes[from];
        int last = rowGroupIndexes[from + count - 1];
        String label = first == last ? "rg=" + first : "rg=" + first + "-" + last;
        return new IndexWindow(inputFile, firstWorkItemIndex + from,
                Arrays.copyOfRange(rowGroups, from, from + count),
                Arrays.copyOfRange(offsetIndexColumns, from, from + count),
                Arrays.copyOfRange(columnIndexColumns, from, from + count),
                Arrays.copyOfRange(bloomFilterColumns, from, from + count), label);
    }

    /// Adds the bloom-filter slices of `columns` whose extent the footer gives. A filter without
    /// one is read by [RowGroupBloomFilterSource] if a probe reaches it.
    private static void addBloomFilterRanges(RowGroup rowGroup, BitSet columns,
            CoalescedRanges.Builder bloomFilters) {
        List<ColumnChunk> chunks = rowGroup.columns();
        for (int c = columns.nextSetBit(0); c >= 0; c = columns.nextSetBit(c + 1)) {
            ColumnMetaData metaData = chunks.get(c).metaData();
            if (RowGroupBloomFilterSource.hasKnownExtent(metaData)) {
                bloomFilters.add(metaData.bloomFilterOffset(), metaData.bloomFilterLength());
            }
        }
    }

    /// The bytes fetching the window reads: its slices of the three structures plus the gaps the
    /// merges bridge between them.
    long fetchedBytes() {
        return offsetIndexes.fetchedBytes() + columnIndexes.fetchedBytes()
                + bloomFilters.fetchedBytes();
    }

    int memberCount() {
        return rowGroups.length;
    }

    /// The index buffers of the work item at `workItemIndex`, fetching the whole window on the
    /// first call. A failed fetch stores nothing, so the next call fetches again.
    ///
    /// @throws IllegalStateException if the work item is not a member, or was released
    synchronized RowGroupIndexBuffers buffersFor(int workItemIndex) throws IOException {
        int member = member(workItemIndex);
        if (released[member]) {
            throw new IllegalStateException("Page index of work item " + workItemIndex
                    + " requested after its release");
        }
        if (buffers == null) {
            fetch();
        }
        return buffers[member];
    }

    /// The bloom-filter bytes of the work item at `workItemIndex`, one entry per column of its row
    /// group: the filter's bytes for a column this window fetched it for, `null` for every other.
    /// Fetches the window's bloom filters on the first call that needs them. A failed fetch stores
    /// nothing, so the next call fetches again.
    ///
    /// @throws IllegalStateException if the work item is not a member, or was released
    synchronized ByteBuffer[] bloomFiltersFor(int workItemIndex) throws IOException {
        int member = member(workItemIndex);
        if (released[member]) {
            throw new IllegalStateException("Bloom filters of work item " + workItemIndex
                    + " requested after their release");
        }
        List<ColumnChunk> chunks = rowGroups[member].columns();
        ByteBuffer[] result = new ByteBuffer[chunks.size()];
        BitSet columns = bloomFilterColumns[member];
        for (int c = columns.nextSetBit(0); c >= 0; c = columns.nextSetBit(c + 1)) {
            ColumnMetaData metaData = chunks.get(c).metaData();
            if (RowGroupBloomFilterSource.hasKnownExtent(metaData)) {
                if (!bloomFilters.isFetched()) {
                    try (FetchReason.Scope ignored = FetchReason.set(rowGroupLabel + " pruning")) {
                        bloomFilters.fetch(inputFile);
                    }
                }
                result[c] = bloomFilters.slice(metaData.bloomFilterOffset(), metaData.bloomFilterLength());
            }
        }
        return result;
    }

    /// Releases the work item at `workItemIndex`, dropping the window's bytes once every member is
    /// released. Releasing a member twice has no further effect.
    synchronized void release(int workItemIndex) {
        int member = member(workItemIndex);
        if (!released[member]) {
            released[member] = true;
            unreleased--;
            if (unreleased == 0) {
                drop();
            }
        }
    }

    /// Releases every member, as the read closes.
    synchronized void releaseAll() {
        Arrays.fill(released, true);
        unreleased = 0;
        drop();
    }

    private void fetch() throws IOException {
        try (FetchReason.Scope ignored = FetchReason.set(rowGroupLabel + " indexes")) {
            if (!offsetIndexes.isFetched()) {
                offsetIndexes.fetch(inputFile);
            }
            if (!columnIndexes.isFetched()) {
                columnIndexes.fetch(inputFile);
            }
        }
        RowGroupIndexBuffers[] sliced = new RowGroupIndexBuffers[rowGroups.length];
        for (int m = 0; m < rowGroups.length; m++) {
            sliced[m] = RowGroupIndexBuffers.slice(rowGroups[m], offsetIndexColumns[m],
                    columnIndexColumns[m], offsetIndexes, columnIndexes);
        }
        buffers = sliced;
    }

    private void drop() {
        buffers = null;
        offsetIndexes = null;
        columnIndexes = null;
        bloomFilters = null;
    }

    private int member(int workItemIndex) {
        int member = workItemIndex - firstWorkItemIndex;
        if (member < 0 || member >= rowGroups.length) {
            throw new IllegalStateException("Work item " + workItemIndex
                    + " is not in the index window of work items " + firstWorkItemIndex + " to "
                    + (firstWorkItemIndex + rowGroups.length - 1));
        }
        return member;
    }
}
