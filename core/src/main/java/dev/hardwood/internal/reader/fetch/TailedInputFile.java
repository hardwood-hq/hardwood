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
import java.util.Optional;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.io.ReadRanges;
import dev.hardwood.internal.reader.memory.InternalProperty;

/// An opened file whose bytes are not in memory, together with its tail: the last
/// [#tailBytes()] bytes, or the whole file where it is shorter, read in one request by the
/// file's footer entry. The tail holds the footer and usually the page index, and a file up to
/// that size whole, so every read that falls inside it is sliced from it and costs no request
/// ([#readRange]), until the read is done with the file ([#dropTail]).
///
/// An S3 object's open fetches its tail with the request that learns its length, so taking it
/// here costs no request (`S3InputFile`), and the tail is then held here rather
/// than by the file.
public final class TailedInputFile implements InputFile {

    public static final String TAIL_BYTES_PROPERTY = "hardwood.internal.tailBytes";

    /// The tail a read takes without the override.
    static final int DEFAULT_TAIL_BYTES = 256 * 1024;

    /// The trailer: the footer length and the trailing magic, which every tail holds.
    private static final int TRAILER_BYTES = 8;

    private final InputFile delegate;
    private final long tailOffset;
    /// The tail's bytes, `null` once dropped.
    private volatile ByteBuffer tail;

    private TailedInputFile(InputFile delegate, long tailOffset, ByteBuffer tail) {
        this.delegate = delegate;
        this.tailOffset = tailOffset;
        this.tail = tail.slice();
    }

    /// The tail a read takes, [#DEFAULT_TAIL_BYTES] unless overridden through
    /// [#TAIL_BYTES_PROPERTY], read for each file. A value below the eight bytes of the trailer
    /// is rejected rather than replaced by the default.
    public static int tailBytes() {
        return InternalProperty.intValue(TAIL_BYTES_PROPERTY, DEFAULT_TAIL_BYTES, TRAILER_BYTES,
                "a byte count of at least " + TRAILER_BYTES);
    }

    /// Reads the tail of the opened `inputFile` in one request, under the `footer-info` reason;
    /// a file whose bytes are in memory is returned as it is, since it slices every read in place.
    ///
    /// @throws IOException if the read fails
    public static InputFile take(InputFile inputFile) throws IOException {
        if (inputFile instanceof TailedInputFile || RangeRead.inMemory(inputFile)) {
            return inputFile;
        }
        long length = inputFile.length();
        int tailLength = Math.toIntExact(Math.min(tailBytes(), length));
        ByteBuffer tail;
        try (FetchReason.Scope ignored = FetchReason.set("footer-info")) {
            tail = ReadRanges.read(inputFile, length - tailLength, tailLength);
        }
        return new TailedInputFile(inputFile, length - tailLength, tail);
    }

    /// The offset of the tail's first byte in a file whose tail `inputFile` holds, and
    /// [Long#MAX_VALUE] for any other file or once the tail is dropped.
    public static long tailOffset(InputFile inputFile) {
        return inputFile instanceof TailedInputFile tailed && tailed.tail != null
                ? tailed.tailOffset
                : Long.MAX_VALUE;
    }

    /// A slice of the tail `inputFile` holds for a range inside it, `null` for a range outside it,
    /// once the tail is dropped, or for any other file: never a request.
    static ByteBuffer sliceTail(InputFile inputFile, long offset, int length) {
        if (!(inputFile instanceof TailedInputFile tailed)) {
            return null;
        }
        ByteBuffer held = tailed.tail;
        if (held == null || length < 0 || offset < tailed.tailOffset
                || offset + length > tailed.tailOffset + held.limit()) {
            return null;
        }
        return held.slice(Math.toIntExact(offset - tailed.tailOffset), length);
    }

    /// Drops the tail of a file whose tail `inputFile` holds, once the read is done with the
    /// file; a read of it from then on is a request again. No-op for any other file.
    public static void dropTail(InputFile inputFile) {
        if (inputFile instanceof TailedInputFile tailed) {
            tailed.tail = null;
        }
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    /// A slice of the tail for a range inside it, without a request, and otherwise the file's own
    /// read.
    @Override
    public ByteBuffer readRange(long offset, int length) throws IOException {
        ByteBuffer slice = sliceTail(this, offset, length);
        return slice != null ? slice : delegate.readRange(offset, length);
    }

    @Override
    public long length() throws IOException {
        return delegate.length();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public Optional<String> identity() throws IOException {
        return delegate.identity();
    }

    /// Closes nothing: the file belongs to the caller that opened it.
    @Override
    public void close() {
        // The delegate is closed by its owner
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
