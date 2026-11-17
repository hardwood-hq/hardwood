/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

/// The charge of something a read holds, such as a range's bytes or a file's tail: the bytes the
/// scheduler charged as its job started, whether they count against F, and its readers, the last of
/// which hands it back to the scheduler ([JobScheduler#release(Charge)]). Used under the read's
/// lock.
final class Charge {

    private long bytes;
    private boolean ahead;
    private int readers;
    /// for a decode need forced that did not fit its ledger: the bytes it needed, the total they
    /// would have taken the ledger to, and its limit; `0` otherwise
    private long refusedNeeded;
    private long refusedTotal;
    private long refusedLimit;

    /// Takes `bytes` the scheduler charged as a job started; `ahead` where they count against F.
    void take(long bytes, boolean ahead) {
        this.bytes += bytes;
        this.ahead = ahead;
    }

    /// One more reader, before the charge can be returned.
    void reader() {
        readers++;
    }

    /// Whether the bytes count against F.
    boolean ahead() {
        return ahead;
    }

    /// The decode need forced, needing `needed` bytes, did not fit: they would have taken the ledger
    /// to `total`, past `limit`.
    void refused(long needed, long total, long limit) {
        this.refusedNeeded = needed;
        this.refusedTotal = total;
        this.refusedLimit = limit;
    }

    /// Whether the decode need forced did not fit its ledger ([#refused]).
    boolean isRefused() {
        return refusedTotal > 0;
    }

    /// The bytes the refused decode needed.
    long refusedNeeded() {
        return refusedNeeded;
    }

    /// The total the refused decode would have taken its ledger to.
    long refusedTotal() {
        return refusedTotal;
    }

    /// The limit of the ledger that refused it.
    long refusedLimit() {
        return refusedLimit;
    }

    /// The bytes it holds.
    long bytes() {
        return bytes;
    }

    /// Hands the bytes back: returns them, and holds none from then on.
    long takeBack() {
        long returned = bytes;
        bytes = 0;
        return returned;
    }

    /// A reader is done; returns whether it was the last, after which the charge is returned.
    boolean readerDone() {
        return --readers == 0;
    }
}
