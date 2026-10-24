/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.util.function.Consumer;

/// The read of one file's footer, as a request of a read's [FetchSequence]: the footer's
/// counterpart of a [SectionRequest], positioned ahead of the file's section requests and charged
/// to R at the tail its read takes. Its owner
/// ([dev.hardwood.internal.reader.plan.FooterReadAhead]) decides which files' footers are
/// requested and what starting one does.
public final class FooterRequest extends Request {

    private final Consumer<FooterRequest> start;

    /// @param charge the bytes the request takes from R when it starts ahead of need
    /// @param start what starting the request does, run once the sequence has let go of its lock
    public FooterRequest(int fileIndex, int charge, Consumer<FooterRequest> start) {
        super(fileIndex, charge);
        this.start = start;
    }

    public int fileIndex() {
        return fileIndex;
    }

    @Override
    Runnable begin() {
        return () -> start.accept(this);
    }
}
