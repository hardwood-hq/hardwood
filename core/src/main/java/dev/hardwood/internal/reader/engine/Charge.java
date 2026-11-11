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

    long bytes;
    boolean ahead;
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

    /// A reader is done; returns whether it was the last, after which the charge is returned.
    boolean readerDone() {
        return --readers == 0;
    }
}
