/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.function.IntToLongFunction;

/// The rows a unit not yet planned is taken to keep, for the look-ahead: the fraction of its rows
/// the unit planned last kept, at least a batch's rows, and every row before any unit is planned;
/// and the key the look-ahead's kept rows end at over the units not yet batched ([#keyPast]). The
/// floor bounds how many units a read plans ahead of its look-ahead's kept rows where units keep
/// no row. Used under the read's lock.
final class KeptRowsEstimate {

    private final long floor;
    /// the rows the unit planned last kept, `-1` before any is planned, and its rows
    private long lastKept = -1;
    private long lastRows;

    KeptRowsEstimate(long floor) {
        this.floor = floor;
    }

    /// A unit of `rows` rows is planned, keeping `kept` of them.
    void planned(long kept, long rows) {
        lastKept = kept;
        lastRows = rows;
    }

    /// The key `rows` kept rows end at over units `from` to `to` (exclusive) in unit order, each
    /// keyed from `base` and holding `unitRows` rows: a planned unit counted by the rows it keeps
    /// (`plannedKept`, `-1` for a unit not yet planned), any other by the rows it is taken to keep
    /// ([#of]), and past the last every row from `endKey`. A unit not yet planned counts at least
    /// the floor, so the walk visits at most the rows over the floor of them, beside the planned
    /// units not yet batched.
    long keyPast(int from, int to, IntToLongFunction base, IntToLongFunction unitRows, IntToLongFunction plannedKept,
                 long endKey, long rows) {
        long left = rows;
        for (int i = from; i < to; i++) {
            long planned = plannedKept.applyAsLong(i);
            long kept = planned >= 0 ? planned : of(unitRows.applyAsLong(i));
            if (kept >= left) {
                return base.applyAsLong(i) + left;
            }
            left -= kept;
        }
        return endKey + left;
    }

    /// The rows a unit of `rows` rows not yet planned is taken to keep.
    long of(long rows) {
        if (lastKept < 0 || lastRows == 0) {
            return rows;
        }
        long estimate = (long) ((double) rows * lastKept / lastRows);
        return Math.min(rows, Math.max(floor, estimate));
    }
}
