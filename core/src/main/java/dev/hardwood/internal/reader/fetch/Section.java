/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.io.IOException;
import java.nio.ByteBuffer;

import dev.hardwood.internal.reader.step.Need;

/// A range of a file that a walk reads its pages from: the bytes of one request of the read,
/// present or not. A walk asks for them without waiting ([#poll]); while they are absent, the
/// section is what the walk needs ([PageWalk#pending]).
public interface Section extends Need {

    /// The section's first byte in the file.
    long offset();

    /// The file offset one past the section's last byte.
    long end();

    /// The section's bytes, or `null` while they are not present, without waiting.
    ///
    /// @throws IOException if the read of the bytes failed
    ByteBuffer poll() throws IOException;

    /// A slice of `length` bytes at absolute `fileOffset` from bytes [#poll] returned.
    ///
    /// @throws IllegalArgumentException if the range lies outside the section
    ByteBuffer slice(ByteBuffer polled, long fileOffset, int length);

    /// Records that one of the columns holding the section has passed it.
    void release();

    /// Starts the read of the section's bytes now, if it has not started.
    void demand();

    /// Releases the section whatever its holders, as the read closes.
    void discard();
}
