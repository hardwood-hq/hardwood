/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

/// A request of a read, at a position in its [FetchSequence]: a [SectionRequest], a range of a
/// file, or a [FooterRequest], a file's footer, which the read queues ([FetchSequence#queue])
/// ahead of the file's section requests.
public abstract sealed class Request permits SectionRequest, FooterRequest {

    /// The file of a request that is never queued, which needs no position.
    static final int UNQUEUED = -1;

    final int fileIndex;
    final int workItemIndex;
    final long offset;
    final int length;
    /// Whether a speculative start charges the request to F as well as to R: a section
    /// request's does.
    final boolean chargesF;
    /// Guarded by the sequence's lock: whether the request holds a charge.
    boolean charged;
    /// Guarded by the sequence's lock: whether the request waits in the queue.
    boolean queued;
    /// Guarded by the sequence's lock: whether the request is released.
    boolean released;
    /// Written under the sequence's lock: whether the request has started.
    volatile boolean started;

    /// A request of `length` bytes, the charge it takes from R when it starts ahead of need,
    /// positioned ahead of every section request of file `fileIndex`.
    Request(int fileIndex, int length) {
        this(fileIndex, FetchSequence.PLANNING, 0, length, false);
    }

    Request(int fileIndex, int workItemIndex, long offset, int length, boolean chargesF) {
        this.fileIndex = fileIndex;
        this.workItemIndex = workItemIndex;
        this.offset = offset;
        this.length = length;
        this.chargesF = chargesF;
    }

    /// Starts the request. Called under the sequence's lock, which it takes no other lock under.
    ///
    /// @return what starts its I/O, which the sequence runs once it has let go of the lock
    abstract Runnable begin();

    /// Whether the request has started.
    public boolean started() {
        return started;
    }

    /// Drops what the request holds once it is released. Called under the sequence's lock.
    void dropped() {
    }

    int towardF() {
        return chargesF ? length : 0;
    }
}
