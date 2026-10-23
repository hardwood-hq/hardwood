/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Bytes a [PageWalk] needs and does not hold, which [PageWalk#pending] returns: a chunk or a
/// region whose read has not completed, or whose read was speculative and failed. The walk issues no read for them; the step that drives it
/// does ([#issue]) and yields on the read, and a thread off the pool waits for it.
public interface ByteNeed extends Need {

    /// The read of the bytes: the one under way, or one started as demand when none is under way
    /// or a speculative one failed.
    RangeRead issue();

    /// Issues the read ([#issue]) and runs `resume` once it has completed.
    @Override
    default void whenMet(Runnable resume) {
        issue().whenDone(resume);
    }
}
