/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.io.ReadRanges;
import dev.hardwood.internal.predicate.RowGroupBloomFilterSource;
import dev.hardwood.internal.reader.fetch.CoalescedRanges;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.fetch.Span;
import dev.hardwood.internal.reader.memory.InternalProperty;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetReadException;

/// The page-index and bloom-filter slices of a run of consecutive row groups of one file, read
/// together: the ColumnIndex slices of all members merged into requests of their own
/// ([CoalescedRanges]), the OffsetIndex slices into others, the bloom-filter slices into a third
/// set. A window takes as many row groups as fit its budget, so a file whose page index fits it is
/// one window; the read fetches each window's requests ([#indexSpans], [#bloomFilterSpans]) as its
/// members' planning needs them.
public final class IndexWindow {

    /// A work item as its window sees it: its row group and the columns whose index structures
    /// it needs.
    ///
    /// @param rowGroupIndex the row group's index in the file, for the fetch reason
    /// @param offsetIndexColumns the columns whose OffsetIndex it needs
    /// @param columnIndexColumns the columns whose ColumnIndex it needs
    /// @param bloomFilterColumns the columns whose bloom filter it may probe
    public record Member(RowGroup rowGroup, int rowGroupIndex, BitSet offsetIndexColumns, BitSet columnIndexColumns,
                  BitSet bloomFilterColumns) {
    }

    private final List<Member> members;
    private final String rowGroupLabel;
    private final CoalescedRanges offsetIndexes;
    private final CoalescedRanges columnIndexes;
    private final CoalescedRanges bloomFilters;
    private IndexWindow(InputFile inputFile, List<Member> members) {
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

    /// Splits one file's row groups into consecutive windows. Each window takes as many of the
    /// following row groups as fit its fetched bytes within `budgetBytes`, and at least one, so a
    /// file whose page index fits the budget is one window.
    ///
    /// @param budgetBytes the most bytes one window fetches, gaps included, unless a single row
    ///        group needs more
    /// @param members the row groups, in read order
    /// @throws ParquetReadException if a page index or bloom filter a member reads lies outside the
    ///         file
    public static List<IndexWindow> plan(long budgetBytes, InputFile inputFile, List<Member> members)
            throws IOException {
        requireInFile(inputFile, members);
        List<IndexWindow> windows = new ArrayList<>();
        int from = 0;
        while (from < members.size()) {
            IndexWindow window = largest(budgetBytes, inputFile, members, from);
            windows.add(window);
            from += window.memberCount();
        }
        return windows;
    }

    /// Fails unless every page index and bloom filter `members` read lies within the file, so a
    /// footer that misplaces one fails as such before any window requests it.
    private static void requireInFile(InputFile inputFile, List<Member> members) throws IOException {
        long fileLength = inputFile.length();
        for (Member member : members) {
            List<ColumnChunk> chunks = member.rowGroup().columns();
            BitSet columns = (BitSet) member.offsetIndexColumns().clone();
            columns.or(member.columnIndexColumns());
            columns.or(member.bloomFilterColumns());
            for (int c = columns.nextSetBit(0); c >= 0; c = columns.nextSetBit(c + 1)) {
                ColumnChunk chunk = chunks.get(c);
                ColumnMetaData metaData = chunk.metaData();
                String column = metaData.pathInSchema().toString();
                if (member.offsetIndexColumns().get(c) && chunk.offsetIndexOffset() != null) {
                    ReadRanges.requireInFile(inputFile.name(), member.rowGroupIndex(), column, "offset index",
                            chunk.offsetIndexOffset(), chunk.offsetIndexOffset() + chunk.offsetIndexLength(),
                            fileLength);
                }
                if (member.columnIndexColumns().get(c) && chunk.columnIndexOffset() != null) {
                    ReadRanges.requireInFile(inputFile.name(), member.rowGroupIndex(), column, "column index",
                            chunk.columnIndexOffset(), chunk.columnIndexOffset() + chunk.columnIndexLength(),
                            fileLength);
                }
                if (member.bloomFilterColumns().get(c) && RowGroupBloomFilterSource.hasKnownExtent(metaData)) {
                    ReadRanges.requireInFile(inputFile.name(), member.rowGroupIndex(), column, "bloom filter",
                            metaData.bloomFilterOffset(), metaData.bloomFilterOffset() + metaData.bloomFilterLength(),
                            fileLength);
                }
            }
        }
    }

    /// The window over the most row groups from `from` on that fits the budget, and at least one.
    /// Fetched bytes grow with every member added, so the count is found by galloping: counts of
    /// 1, 2, 4, … until one exceeds the budget or reaches the file's end, then bisection between
    /// the last that fit and the first that did not. Every candidate is at most twice the size of
    /// the window it finds, so planning a file costs time linear in its row groups.
    private static IndexWindow largest(long budgetBytes, InputFile inputFile, List<Member> members, int from) {
        int remaining = members.size() - from;
        IndexWindow best = window(inputFile, members, from, 1);
        int fits = 1;
        int exceeds = remaining + 1;
        while (fits < remaining) {
            int count = (int) Math.min((long) fits * 2, remaining);
            IndexWindow candidate = window(inputFile, members, from, count);
            if (candidate.fetchedBytes() > budgetBytes) {
                exceeds = count;
                break;
            }
            best = candidate;
            fits = count;
        }
        while (exceeds - fits > 1) {
            int mid = (fits + exceeds) >>> 1;
            IndexWindow candidate = window(inputFile, members, from, mid);
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

    private static IndexWindow window(InputFile inputFile, List<Member> members, int from, int count) {
        return new IndexWindow(inputFile, members.subList(from, from + count));
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

    public int memberCount() {
        return members.size();
    }

    /// The most bytes one window fetches, gaps included, unless a single row group needs more.
    public static long budgetBytes() {
        return InternalProperty.longValue("hardwood.internal.indexWindowBytes", 16L * 1024 * 1024, 1,
                "a positive byte count");
    }

    /// The window's page-index requests, OffsetIndex and ColumnIndex, each held by every member.
    public List<Span> indexSpans() {
        return spans("indexes", offsetIndexes, columnIndexes);
    }

    /// The window's bloom-filter requests, each held by every member.
    public List<Span> bloomFilterSpans() {
        return spans("pruning", bloomFilters);
    }

    private List<Span> spans(String reason, CoalescedRanges... structures) {
        List<Span> spans = new ArrayList<>();
        for (CoalescedRanges ranges : structures) {
            for (int r = 0; r < ranges.requestCount(); r++) {
                long start = ranges.requestStart(r);
                spans.add(new Span(start, Math.toIntExact(ranges.requestEnd(r) - start),
                        rowGroupLabel + " " + reason, members.size()));
            }
        }
        return spans;
    }
}
