/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Predicate;

import dev.hardwood.InputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.decode.PageFormatProbe;
import dev.hardwood.internal.reader.step.ByteSource;
import dev.hardwood.reader.ParquetReadException;

/// Walks a column chunk's pages header to header, for the commands and `dive` screens that list
/// them at the byte level: the reader's walk ([PageFormatProbe#walk]) over the file read in
/// windows.
public final class PageHeaderWalk {

    /// Largest single read of a walk. A chunk up to this size is one read; a larger one,
    /// including one past what a `readRange` can address, is walked in windows.
    public static final int WINDOW_BYTES = 64 * 1024 * 1024;

    private PageHeaderWalk() {
    }

    /// Reads the headers of the pages in `[start, start + totalBytes)` in order, handing each to
    /// `visitor` until it returns `false` or the range ends.
    ///
    /// Reads are at most `windowBytes`, unless a header needs more. A header the current window
    /// does not hold starts the next window, so a body longer than a window is skipped rather than
    /// read. A header cut off by the range's end is a truncated chunk, and a page whose body runs
    /// past it a malformed one; both fail as the reader fails them.
    ///
    /// @param visitor receives each header; returns whether the walk goes on
    /// @throws IOException if a read fails
    /// @throws ParquetReadException if a header exceeds [PageFormatProbe#MAX_PEEK_SIZE], is cut off
    ///         by the range's end, or its page runs past it
    public static void walk(InputFile inputFile, long start, long totalBytes, int windowBytes,
            Predicate<PageHeader> visitor) throws IOException {
        long end = start + totalBytes;
        PageFormatProbe.walk(windows(inputFile, end, windowBytes), start, end, visitor);
    }

    /// The bytes of `inputFile` up to `end`, read a window of at most `windowBytes` at a time, or
    /// of the range asked for where that is longer; a range the current window does not hold
    /// starts the next one.
    private static ByteSource windows(InputFile inputFile, long end, int windowBytes) {
        return new ByteSource() {

            private long windowStart;
            private ByteBuffer window;

            @Override
            public ByteBuffer read(long offset, int length) throws IOException {
                if (window == null || offset < windowStart || offset + length > windowStart + window.remaining()) {
                    windowStart = offset;
                    window = inputFile.readRange(offset,
                            Math.toIntExact(Math.max(length, Math.min(windowBytes, end - offset))));
                }
                return window.slice(Math.toIntExact(offset - windowStart), length);
            }
        };
    }
}
