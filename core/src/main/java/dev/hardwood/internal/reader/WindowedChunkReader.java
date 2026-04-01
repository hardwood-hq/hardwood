/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;

import dev.hardwood.InputFile;

/// Provides windowed access to a column chunk's byte data.
///
/// Fetches data in windows on demand via [InputFile#readRange]. The window
/// advances as the [PageScanner] progresses, so only the data actually needed
/// is fetched from the network.
///
/// For local files backed by memory-mapped I/O, `readRange()` returns a
/// zero-copy slice — the first window covers the full chunk with no overhead.
///
/// This is the bridge between [PageScanner]'s incremental iteration and the
/// [InputFile] abstraction.
public class WindowedChunkReader {

    /// Default window size (256 KB).
    static final int DEFAULT_WINDOW_SIZE = 256 * 1024;

    private final InputFile inputFile;
    private final long chunkOffset;
    private final int chunkLength;
    private final int windowSize;

    // Current window state
    private ByteBuffer window;
    private int windowStart; // relative offset within the chunk where the window begins
    private int windowEnd;   // relative offset within the chunk where the window ends (exclusive)

    /// Creates a windowed reader for a column chunk.
    ///
    /// @param inputFile the file to read from
    /// @param chunkOffset absolute file offset where the column chunk starts
    /// @param chunkLength total size of the column chunk in bytes
    /// @param windowSize number of bytes to fetch per window
    public WindowedChunkReader(InputFile inputFile, long chunkOffset, int chunkLength, int windowSize) {
        this.inputFile = inputFile;
        this.chunkOffset = chunkOffset;
        this.chunkLength = chunkLength;
        this.windowSize = windowSize;
    }

    /// Creates a windowed reader with the default window size.
    ///
    /// @param inputFile the file to read from
    /// @param chunkOffset absolute file offset where the column chunk starts
    /// @param chunkLength total size of the column chunk in bytes
    public WindowedChunkReader(InputFile inputFile, long chunkOffset, int chunkLength) {
        this(inputFile, chunkOffset, chunkLength, DEFAULT_WINDOW_SIZE);
    }

    /// Returns a [ByteBuffer] covering bytes `[relativeOffset, relativeOffset + length)`
    /// within the column chunk. May trigger I/O if the requested range falls outside
    /// the current window.
    ///
    /// @param relativeOffset offset relative to the start of the column chunk
    /// @param length number of bytes
    /// @return a ByteBuffer positioned at the start of the requested range
    /// @throws IOException if a fetch fails
    public ByteBuffer slice(int relativeOffset, int length) throws IOException {
        if (length > chunkLength - relativeOffset) {
            throw new IndexOutOfBoundsException(
                    "Requested range [" + relativeOffset + ", " + (relativeOffset + length)
                            + ") exceeds chunk length " + chunkLength);
        }

        // Check if current window covers the requested range
        if (window == null || relativeOffset < windowStart || relativeOffset + length > windowEnd) {
            fetchWindow(relativeOffset, length);
        }

        int offsetInWindow = relativeOffset - windowStart;
        return window.slice(offsetInWindow, length);
    }

    /// Total size of the column chunk in bytes.
    public int totalLength() {
        return chunkLength;
    }

    /// Fetches a window that covers at least `[relativeOffset, relativeOffset + minLength)`.
    /// The window starts at `relativeOffset` and extends up to `windowSize` bytes
    /// (or to the end of the chunk, whichever comes first).
    private void fetchWindow(int relativeOffset, int minLength) throws IOException {
        int fetchSize = Math.max(windowSize, minLength);
        int available = chunkLength - relativeOffset;
        fetchSize = Math.min(fetchSize, available);

        long absoluteOffset = chunkOffset + relativeOffset;
        window = inputFile.readRange(absoluteOffset, fetchSize);
        windowStart = relativeOffset;
        windowEnd = relativeOffset + fetchSize;
    }
}
