/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.predicate.RowGroupBloomFilterSource;
import dev.hardwood.internal.reader.fetch.CoalescedRanges;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.fetch.FetchSequence;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;

/// The page-index and bloom-filter slices of a run of consecutive work items of one file, read
/// together: the ColumnIndex slices of all members merged into requests of their own
/// ([CoalescedRanges]), the OffsetIndex slices into others, the bloom-filter slices into a third
/// set.
///
/// Formed while the file is planned, each structure's requests held by every member in a
/// [PlanningReads]. No request starts until the planning of a member needs it: the bloom filters'
/// for the first member that probes them ([#bloomFiltersNeed]), the page index's for the first
/// member its Bloom filters and dictionaries keep ([#indexNeed]). Either starts every request of
/// its structure and returns the first one not present, which the planning waits on before it
/// slices the member's bytes. The requests are released once every member has been released
/// ([#release]), or when the read closes ([#releaseAll]).
final class IndexWindow {

    /// A work item as its window sees it: its row group and the columns whose index structures
    /// it needs.
    ///
    /// @param rowGroupIndex the row group's index in the file, for the fetch reason
    /// @param offsetIndexColumns the columns whose OffsetIndex it needs
    /// @param columnIndexColumns the columns whose ColumnIndex it needs
    /// @param bloomFilterColumns the columns whose bloom filter it may probe
    record Member(RowGroup rowGroup, int rowGroupIndex, BitSet offsetIndexColumns, BitSet columnIndexColumns,
                  BitSet bloomFilterColumns) {
    }

    private final InputFile inputFile;
    private final int firstWorkItemIndex;
    private final List<Member> members;
    private final String rowGroupLabel;
    private final CoalescedRanges offsetIndexes;
    private final CoalescedRanges columnIndexes;
    private final CoalescedRanges bloomFilters;
    /// Each structure's requests; `null` until [#insertEntries].
    private PlanningReads offsetIndexReads;
    private PlanningReads columnIndexReads;
    private PlanningReads bloomFilterReads;

    private IndexWindow(InputFile inputFile, int firstWorkItemIndex, List<Member> members) {
        this.inputFile = inputFile;
        this.firstWorkItemIndex = firstWorkItemIndex;
        this.members = members;
        int first = members.getFirst().rowGroupIndex();
        int last = members.getLast().rowGroupIndex();
        this.rowGroupLabel = first == last ? "rg=" + first : "rg=" + first + "-" + last;

        CoalescedRanges.Builder offsetIndexBuilder = CoalescedRanges.builder();
        CoalescedRanges.Builder columnIndexBuilder = CoalescedRanges.builder();
        CoalescedRanges.Builder bloomFilterBuilder = CoalescedRanges.builder();
        for (Member member : members) {
            RowGroupIndexBuffers.addRanges(member.rowGroup(), member.offsetIndexColumns(),
                    member.columnIndexColumns(), offsetIndexBuilder, columnIndexBuilder);
            addBloomFilterRanges(member.rowGroup(), member.bloomFilterColumns(), bloomFilterBuilder);
        }
        long cut = HeldBytes.heldFrom(inputFile);
        int maxSpan = CoalescingPolicy.maxSpanBytes();
        this.offsetIndexes = offsetIndexBuilder.build(cut, maxSpan);
        this.columnIndexes = columnIndexBuilder.build(cut, maxSpan);
        this.bloomFilters = bloomFilterBuilder.build(cut, maxSpan);
    }

    /// Splits one file's work items into consecutive windows. Each window takes as many of the
    /// following work items as fit its fetched bytes within `budgetBytes`, and at least one, so a
    /// file whose page index fits the budget is one window.
    ///
    /// @param budgetBytes the most bytes one window fetches, gaps included, unless a single row
    ///        group needs more
    /// @param firstWorkItemIndex the work-list index of the first work item
    /// @param members the work items, in work-list order
    static List<IndexWindow> plan(long budgetBytes, InputFile inputFile, int firstWorkItemIndex,
            List<Member> members) {
        List<IndexWindow> windows = new ArrayList<>();
        int from = 0;
        while (from < members.size()) {
            IndexWindow window = largest(budgetBytes, inputFile, firstWorkItemIndex, members, from);
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
    private static IndexWindow largest(long budgetBytes, InputFile inputFile, int firstWorkItemIndex,
            List<Member> members, int from) {
        int remaining = members.size() - from;
        IndexWindow best = window(inputFile, firstWorkItemIndex, members, from, 1);
        int fits = 1;
        int exceeds = remaining + 1;
        while (fits < remaining) {
            int count = (int) Math.min((long) fits * 2, remaining);
            IndexWindow candidate = window(inputFile, firstWorkItemIndex, members, from, count);
            if (candidate.fetchedBytes() > budgetBytes) {
                exceeds = count;
                break;
            }
            best = candidate;
            fits = count;
        }
        while (exceeds - fits > 1) {
            int mid = (fits + exceeds) >>> 1;
            IndexWindow candidate = window(inputFile, firstWorkItemIndex, members, from, mid);
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

    private static IndexWindow window(InputFile inputFile, int firstWorkItemIndex, List<Member> members,
            int from, int count) {
        return new IndexWindow(inputFile, firstWorkItemIndex + from, members.subList(from, from + count));
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
        return members.size();
    }

    /// Inserts the window's requests into `sequence`, each held by every member. Called once, as
    /// the file is planned.
    void insertEntries(FetchSequence sequence) {
        offsetIndexReads = reads(sequence, offsetIndexes, " indexes");
        columnIndexReads = reads(sequence, columnIndexes, " indexes");
        bloomFilterReads = reads(sequence, bloomFilters, " pruning");
    }

    private PlanningReads reads(FetchSequence sequence, CoalescedRanges ranges, String purpose) {
        PlanningReads reads = new PlanningReads(sequence, inputFile, rowGroupLabel + purpose, members.size());
        reads.insert(ranges);
        return reads;
    }

    /// What reading the window's page index waits for: the first of its requests not present,
    /// after starting every one not started; `null` once all are present.
    Need indexNeed() throws IOException {
        Need need = offsetIndexReads.start();
        Need columnIndexNeed = columnIndexReads.start();
        return need != null ? need : columnIndexNeed;
    }

    /// What reading the window's bloom filters waits for, as [#indexNeed].
    Need bloomFiltersNeed() throws IOException {
        return bloomFilterReads.start();
    }

    /// The index buffers of the work item at `workItemIndex`, once [#indexNeed] has none.
    ///
    /// @throws IllegalStateException if the work item is not a member, or an entry is not present
    RowGroupIndexBuffers buffersFor(int workItemIndex) throws IOException {
        int member = member(workItemIndex);
        requirePresent(offsetIndexReads, workItemIndex);
        requirePresent(columnIndexReads, workItemIndex);
        Member window = members.get(member);
        return RowGroupIndexBuffers.slice(window.rowGroup(), window.offsetIndexColumns(),
                window.columnIndexColumns(), (offset, length) -> slice(offsetIndexReads, offset, length),
                (offset, length) -> slice(columnIndexReads, offset, length));
    }

    /// The bloom-filter bytes of the work item at `workItemIndex`, one entry per column of its row
    /// group: the filter's bytes for a column this window reads it for, `null` for every other;
    /// once [#bloomFiltersNeed] has none.
    ///
    /// @throws IllegalStateException if the work item is not a member, or an entry is not present
    ByteBuffer[] bloomFiltersFor(int workItemIndex) throws IOException {
        int member = member(workItemIndex);
        List<ColumnChunk> chunks = members.get(member).rowGroup().columns();
        ByteBuffer[] result = new ByteBuffer[chunks.size()];
        BitSet columns = members.get(member).bloomFilterColumns();
        for (int c = columns.nextSetBit(0); c >= 0; c = columns.nextSetBit(c + 1)) {
            ColumnMetaData metaData = chunks.get(c).metaData();
            if (RowGroupBloomFilterSource.hasKnownExtent(metaData)) {
                result[c] = slice(bloomFilterReads, metaData.bloomFilterOffset(), metaData.bloomFilterLength());
            }
        }
        return result;
    }

    /// Releases the window's requests for one member, which drops them once every member is
    /// released.
    void release(int workItemIndex) {
        member(workItemIndex);
        offsetIndexReads.release();
        columnIndexReads.release();
        bloomFilterReads.release();
    }

    /// Releases every request, as the read closes.
    void releaseAll() {
        offsetIndexReads.discard();
        columnIndexReads.discard();
        bloomFilterReads.discard();
    }

    /// Fails unless `reads` hold every request, as they do once [#indexNeed] or [#bloomFiltersNeed]
    /// has none and until the window is released.
    private static void requirePresent(PlanningReads reads, int workItemIndex) throws IOException {
        if (!reads.present()) {
            throw new IllegalStateException("Page index of work item " + workItemIndex
                    + " is not present: the window was not read, or was released");
        }
    }

    /// The range `[offset, offset + length)` from `reads`, which hold it.
    private static ByteBuffer slice(PlanningReads reads, long offset, int length) throws IOException {
        ByteBuffer bytes = reads.read(offset, length);
        if (bytes == null) {
            throw new IllegalStateException("Range [" + offset + ", " + (offset + length) + ") is not present");
        }
        return bytes;
    }

    private int member(int workItemIndex) {
        int member = workItemIndex - firstWorkItemIndex;
        if (member < 0 || member >= members.size()) {
            throw new IllegalStateException("Work item " + workItemIndex
                    + " is not in the index window of work items " + firstWorkItemIndex + " to "
                    + (firstWorkItemIndex + members.size() - 1));
        }
        return member;
    }
}
