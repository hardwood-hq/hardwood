/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;

import dev.hardwood.schema.ColumnSchema;

/// The batch boundaries of one read, decided once for every decoded column.
///
/// [RowGroupIterator] plans the row groups of a read in work-list order, as it computes their
/// fetch plans, and hands each row group's [RowGroupBatches] to every column's worker. A worker
/// closes a batch where the plan closes it and takes the batch's file name and always-match flag
/// from the plan, so every column of the read closes its batches at the same rows.
///
/// A batch closes:
///
/// - when it holds `capacity` rows;
/// - before the first row of a row group from another file;
/// - before the first row of a row group whose always-match flag differs from the batch's
///   (filtered reads only);
/// - at the end of every row group, in reads that end their batches at row-group boundaries
///   ([#endsBatchesAtRowGroupBoundaries]);
/// - at the row cap, which ends the read. Under a filter the cap counts matching rows, so it is
///   held only while every row group planned so far was proven to match in full, and dropped for
///   the rest of the read at the first row group that keeps rows and was not;
/// - at the end of the read.
///
/// A row group that keeps no row takes no part: it closes no batch and drops no cap. Every input
/// is known once a row group's fetch plans are: the kept rows come from the row ranges its page
/// masks apply, which apply to every decoded column of the row group or to none.
///
/// Planning is synchronized, since the first column to reach a row group plans it on its own
/// retriever thread, or the previous row group's prefetch does on a speculative task.
final class BatchPlan {

    /// The plan of a row group after the row cap ended the read; no column reaches it.
    private static final RowGroupBatches AFTER_END =
            new RowGroupBatches(-1, null, false, false, 0, 0, 0, 0, 0, false);

    private final int capacity;
    private final boolean endsAtRowGroups;
    private final boolean filtered;

    /// Index of the next work item to plan.
    private int nextWorkItem;
    /// Rows of the batch the last planned row group left open, `0` when none is open.
    private int openRows;
    /// File index and always-match flag of the open batch.
    private int openFileIndex = -1;
    private boolean openAlwaysMatches;
    /// Whether the row cap still applies, and how many rows it leaves.
    private boolean capHeld;
    private long capLeft;
    /// Whether the row cap has ended the read.
    private boolean ended;

    /// @param capacity the read's batch capacity, in rows
    /// @param endsAtRowGroups whether every batch ends at the end of its row group
    /// @param filtered whether a filter is installed, so that the always-match flag splits batches
    ///        and the row cap counts matching rows
    /// @param maxRows the row cap, `0` for none
    BatchPlan(int capacity, boolean endsAtRowGroups, boolean filtered, long maxRows) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Batch capacity must be positive, got " + capacity);
        }
        if (maxRows < 0) {
            throw new IllegalArgumentException("Row cap must not be negative, got " + maxRows);
        }
        this.capacity = capacity;
        this.endsAtRowGroups = endsAtRowGroups;
        this.filtered = filtered;
        this.capHeld = maxRows > 0;
        this.capLeft = maxRows;
    }

    /// Whether a read decoding `columns` ends its batches at row-group boundaries: exactly when
    /// one of the columns holds binary values (`BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`, `INT96`),
    /// flat or nested. A batch of binary values then draws on one column chunk's dictionary. The
    /// cut costs every column of the read a batch hand-off per row group, which only a read with
    /// a binary column has a use for.
    ///
    /// @param columns every column the read decodes, predicate columns included
    static boolean endsBatchesAtRowGroupBoundaries(List<ColumnSchema> columns) {
        for (ColumnSchema column : columns) {
            switch (column.type()) {
                case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> {
                    return true;
                }
                case INT32, INT64, FLOAT, DOUBLE, BOOLEAN -> {
                }
            }
        }
        return false;
    }

    int capacity() {
        return capacity;
    }

    /// Plans the batches of the next row group of the read.
    ///
    /// @param workItemIndex the row group's position in the work list, which must be the one after
    ///        the last planned
    /// @param fileIndex the index of the row group's file in the read
    /// @param fileName the name of the row group's file
    /// @param alwaysMatches whether statistics proved every row of the row group matches the filter
    /// @param keptRows the rows of the row group its page masks keep, all of them when none apply
    synchronized RowGroupBatches plan(int workItemIndex, int fileIndex, String fileName,
                                      boolean alwaysMatches, long keptRows) {
        if (workItemIndex != nextWorkItem) {
            throw new IllegalStateException("Batch plan expects work item " + nextWorkItem
                    + " next, got " + workItemIndex);
        }
        if (keptRows < 0) {
            throw new IllegalArgumentException("Kept rows must not be negative, got " + keptRows);
        }
        nextWorkItem++;
        boolean flag = filtered && alwaysMatches;
        if (ended) {
            return AFTER_END;
        }
        if (keptRows == 0) {
            return new RowGroupBatches(workItemIndex, fileName, flag, false, 0, 0, capacity, 0, 0, false);
        }
        if (filtered && !alwaysMatches) {
            capHeld = false;
        }
        boolean closesOpenBatch = openRows > 0
                && (fileIndex != openFileIndex || flag != openAlwaysMatches);
        if (closesOpenBatch) {
            openRows = 0;
        }
        long rows = keptRows;
        boolean endsRead = capHeld && capLeft <= keptRows;
        if (endsRead) {
            rows = capLeft;
            ended = true;
        }
        if (capHeld) {
            capLeft -= rows;
        }

        int firstRows = (int) Math.min(capacity - openRows, rows);
        long rest = rows - firstRows;
        int lastRows = rest == 0 ? firstRows : (int) (rest % capacity == 0 ? capacity : rest % capacity);
        int segmentCount = Math.toIntExact(1 + (rest + capacity - 1) / capacity);
        int rowsInLastBatch = segmentCount == 1 ? openRows + firstRows : lastRows;
        boolean lastOpen = !endsRead && !endsAtRowGroups && rowsInLastBatch < capacity;

        openRows = lastOpen ? rowsInLastBatch : 0;
        openFileIndex = fileIndex;
        openAlwaysMatches = flag;
        return new RowGroupBatches(workItemIndex, fileName, flag, closesOpenBatch, keptRows,
                firstRows, capacity, segmentCount, lastOpen ? -lastRows : lastRows, endsRead);
    }

    /// The batches of one row group, as segments of its kept rows in row order. The first segment
    /// continues the batch the previous row group left open, unless [#closesOpenBatch()] closes
    /// that batch first, and every later segment starts a batch. Every segment closes its batch at
    /// its end, except a last segment [#lastOpen()] leaves open for the next row group.
    ///
    /// @param workItemIndex the row group's position in the work list
    /// @param fileName the file every batch holding rows of the row group is attributed to
    /// @param alwaysMatches the always-match flag of every batch holding rows of the row group
    /// @param closesOpenBatch whether the batch the previous row group left open closes before
    ///        this row group's first row
    /// @param keptRows the rows of the row group its page masks keep
    /// @param firstRows rows of the first segment
    /// @param capacity rows of every segment between the first and the last
    /// @param segmentCount number of segments, `0` when the row group contributes no row
    /// @param signedLastRows rows of the last segment, negated when the segment leaves its batch
    ///        open
    /// @param endsRead whether the row cap ends the read at the end of the last segment
    record RowGroupBatches(int workItemIndex, String fileName, boolean alwaysMatches,
                           boolean closesOpenBatch, long keptRows, int firstRows, int capacity,
                           int segmentCount, int signedLastRows, boolean endsRead) {

        /// Rows of segment `i`.
        int segmentRows(int i) {
            if (i == segmentCount - 1) {
                return Math.abs(signedLastRows);
            }
            return i == 0 ? firstRows : capacity;
        }

        /// Whether segment `i` closes its batch at its end.
        boolean closes(int i) {
            return i < segmentCount - 1 || signedLastRows > 0;
        }

        /// Whether the last segment leaves its batch open for the next row group.
        boolean lastOpen() {
            return segmentCount > 0 && signedLastRows < 0;
        }

        /// Rows of the segments before segment `i`.
        long rowsBefore(int i) {
            if (i == 0) {
                return 0;
            }
            return firstRows + (long) (i - 1) * capacity;
        }

        /// The rows the plan takes from the row group: its kept rows, or fewer where the row cap
        /// ends the read inside it.
        long plannedRows() {
            return segmentCount == 0 ? 0 : rowsBefore(segmentCount - 1) + segmentRows(segmentCount - 1);
        }
    }
}
