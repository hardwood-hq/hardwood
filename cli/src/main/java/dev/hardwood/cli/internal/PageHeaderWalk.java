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
import dev.hardwood.internal.reader.PageFormatProbe;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.internal.thrift.ThriftTruncatedException;
import dev.hardwood.reader.ParquetReadException;

/// Walks a column chunk's pages header to header, for the commands and `dive` screens that list
/// them at the byte level.
public final class PageHeaderWalk {

    /// Largest single read of a walk. A chunk up to this size is one read; a larger one,
    /// including one past what a `readRange` can address, is walked in windows.
    public static final int WINDOW_BYTES = 64 * 1024 * 1024;

    private PageHeaderWalk() {
    }

    /// Reads the headers of the pages in `[start, start + totalBytes)` in order, handing each to
    /// `visitor` until it returns `false` or the range ends.
    ///
    /// Reads are at most `windowBytes`. A window ends where the next page no longer fits; the
    /// next read starts at that page's header, so a body longer than a window is skipped rather
    /// than read. A header cut off by the window's end is read again at the start of the next
    /// window; a header that does not fit a whole window widens the window, up to
    /// [PageFormatProbe#MAX_PEEK_SIZE]. A header cut off by the range's end is a truncated chunk.
    ///
    /// @param visitor receives each header; returns whether the walk goes on
    /// @throws IOException if a read fails
    /// @throws ParquetReadException if a header exceeds [PageFormatProbe#MAX_PEEK_SIZE]
    public static void walk(InputFile inputFile, long start, long totalBytes, int windowBytes,
            Predicate<PageHeader> visitor) throws IOException {
        long end = start + totalBytes;
        long position = start;
        int window = windowBytes;
        while (position < end) {
            int windowLength = Math.toIntExact(Math.min(end - position, window));
            ByteBuffer buffer = inputFile.readRange(position, windowLength);
            long relative = 0;
            while (relative < windowLength) {
                ThriftCompactReader tcr = new ThriftCompactReader(buffer, Math.toIntExact(relative));
                PageHeader header;
                try {
                    header = PageHeaderReader.read(tcr);
                }
                catch (ThriftTruncatedException e) {
                    if (relative > 0) {
                        break;
                    }
                    if (windowLength == end - position) {
                        throw e;
                    }
                    if (windowLength >= PageFormatProbe.MAX_PEEK_SIZE) {
                        throw new ParquetReadException("Page header at offset " + position
                                + " exceeds maximum peek size (" + PageFormatProbe.MAX_PEEK_SIZE + " bytes)", e);
                    }
                    window = Math.min(2 * window, PageFormatProbe.MAX_PEEK_SIZE);
                    break;
                }
                if (!visitor.test(header)) {
                    return;
                }
                relative += tcr.getBytesRead() + (long) header.compressedPageSize();
            }
            position += relative;
        }
    }
}
