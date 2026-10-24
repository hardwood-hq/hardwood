/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;

/// Validation of the ranges requested from an [InputFile], shared by its implementations
/// so that every backend rejects a bad range with the same exception and message, and of the
/// buffers a backend returns, which every read of the reader checks ([#read]).
public final class ReadRanges {

    private ReadRanges() {
    }

    /// Checks that `[offset, offset + length)` lies within a file of `fileBytes` bytes.
    ///
    /// The comparison is written so that `offset + length` cannot overflow.
    ///
    /// @param name      the file's name, for the message prefix
    /// @param offset    the requested offset
    /// @param length    the requested length
    /// @param fileBytes the file's length in bytes
    /// @throws IndexOutOfBoundsException if the range is negative or extends past the file
    public static void checkBounds(String name, long offset, int length, long fileBytes) {
        if (offset < 0 || length < 0 || offset > fileBytes - length) {
            throw new IndexOutOfBoundsException(ExceptionContext.filePrefix(name)
                    + "readRange(" + offset + ", " + length
                    + ") out of bounds (" + fileBytes + " bytes)");
        }
    }

    /// The bytes `[offset, offset + length)` of `inputFile`, as [InputFile#readRange] returns them:
    /// at position `0`, with exactly `length` bytes remaining. The reader slices what it holds by
    /// absolute index, so every read the reader makes of a file goes through here.
    ///
    /// @throws IOException if the read fails, or if the buffer `inputFile` returns does not hold
    ///         the range as the contract requires
    public static ByteBuffer read(InputFile inputFile, long offset, int length) throws IOException {
        ByteBuffer bytes = inputFile.readRange(offset, length);
        if (bytes.position() != 0 || bytes.remaining() != length) {
            throw new IOException(ExceptionContext.filePrefix(inputFile.name()) + "readRange(" + offset + ", "
                    + length + ") returned a buffer at position " + bytes.position() + " with "
                    + bytes.remaining() + " bytes remaining, not at position 0 with " + length + " bytes remaining");
        }
        return bytes;
    }
}
