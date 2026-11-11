/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

/// The charge of something a read holds, such as a range's bytes or a file's tail, returned
/// once nothing reads it any more. Used under the read's lock.
final class Charge {

    private long bytes;
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

    /// A reader is done; the last returns the charge. Returns whether it was the last.
    boolean read(JobScheduler scheduler) {
        if (--readers == 0) {
            returnTo(scheduler);
            return true;
        }
        return false;
    }

    /// Returns the charge now, whatever its readers.
    void returnTo(JobScheduler scheduler) {
        scheduler.release(bytes, ahead);
        bytes = 0;
    }
}
