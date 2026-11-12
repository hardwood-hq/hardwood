/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

/// The read's failure: the first row the failed work served, the column it was met in, and whether
/// it has settled. The lowest row wins, and of two failures at one row the lower column's, the one a
/// reader taking its columns in lockstep meets first; an `Error` is the JVM's and no row's, and wins
/// at once. Until no work at or before its row is left, a lower failure can still take its place,
/// so no column meets it before it settles. Used under the read's lock.
final class ReadFailure {

    private Throwable cause;
    private long row = Long.MAX_VALUE;
    /// the column the failure was met in, `-1` for none
    private int column = -1;
    private boolean settled;

    /// The read fails at `key`, the first row the failed work served, in no column.
    void failAt(long key, Throwable t) {
        failAt(key, -1, t);
    }

    /// The read fails at `key`, met in `column`.
    void failAt(long key, int column, Throwable t) {
        if (t instanceof Error) {
            key = Long.MIN_VALUE;
        }
        if (key < row || key == row && column < this.column) {
            row = key;
            this.column = column;
            cause = t;
            settled = false;
        }
    }

    /// Settles the failure once `pending` says no work at or before its row is left; returns
    /// whether it settled now, after which every column meets it.
    boolean settle(boolean pending) {
        if (cause != null && !settled && !pending) {
            settled = true;
            return true;
        }
        return false;
    }

    boolean settled() {
        return settled;
    }

    boolean failed() {
        return cause != null;
    }

    /// The first row the failure served, `Long.MAX_VALUE` while the read has not failed.
    long row() {
        return row;
    }

    /// What the read failed with, `null` while it has not.
    Throwable cause() {
        return cause;
    }
}
