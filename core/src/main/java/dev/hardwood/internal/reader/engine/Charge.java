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
/// which hands it back to the scheduler ([JobScheduler#release(Charge)]); and the bytes a job took
/// from R itself as it ran ([#tookItself]), returned with the job's end or, for a job whose end is
/// never applied, by the close of its read ([JobScheduler#releaseAll]), whichever comes first. Used
/// under the read's lock, but for the bytes a job took itself, which its own thread records.
final class Charge {

    private long bytes;
    /// the bytes the job took from R itself; set on the job's thread, taken back under the read's
    /// lock by the job's end or the read's close, so taken and returned through the charge's monitor
    private long tookItself;
    private boolean ahead;
    private int readers;

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

    /// The job took `bytes` from R itself, on its own thread, which its end returns.
    synchronized void tookItself(long bytes) {
        tookItself += bytes;
    }

    /// Hands back what the job took from R itself: returns it, and holds none from then on.
    synchronized long takeBackItself() {
        long returned = tookItself;
        tookItself = 0;
        return returned;
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
