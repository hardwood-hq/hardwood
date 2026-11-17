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
    /// whether its bytes are a running task's buffers, which the ledger counts as transient
    private boolean transientBuffers;
    private int readers;
    /// for a charge need forced that did not fit its ledger: the total it would have taken the
    /// ledger to; `0` otherwise
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

    /// Marks its bytes as a running task's buffers.
    void transientBuffers() {
        this.transientBuffers = true;
    }

    /// Whether its bytes are a running task's buffers.
    boolean isTransient() {
        return transientBuffers;
    }

    /// Whether the bytes count against F.
    boolean ahead() {
        return ahead;
    }

    /// The charge need forced, `needed` bytes, did not fit: it would have taken the ledger to
    /// `total`, past `limit`.
    void refused(long needed, long total, long limit) {
        this.refusedNeeded = needed;
        this.refusedTotal = total;
        this.refusedLimit = limit;
    }

    /// Whether the charge need forced did not fit its ledger ([#refused]).
    boolean isRefused() {
        return refusedTotal > 0;
    }

    /// The bytes of the refused charge.
    long refusedNeeded() {
        return refusedNeeded;
    }

    /// The total the refused charge would have taken its ledger to.
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
