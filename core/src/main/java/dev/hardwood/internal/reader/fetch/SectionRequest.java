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

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.io.ReadRanges;

/// One data request of a read: a range of the file the needed ranges of one or more columns of
/// a row group were coalesced into. A walk asks for its bytes with [#poll], which never waits
/// and issues no read; while they are not present the section request is the [ByteNeed] the
/// step issues ([#issue]), which starts it on demand or joins the read under way.
public final class SectionRequest extends Request implements ByteNeed {

    /// `null` for a section request sliced in place, which no sequence holds.
    final FetchSequence sequence;
    final InputFile inputFile;
    private final String reason;
    /// Guarded by the sequence's lock: the columns that have not passed the section request.
    int holders;
    /// The read under way or done; `null` before it starts and once released. Replaced only to
    /// repeat a read started ahead of demand that failed with an exception.
    volatile RequestAttempt<ByteBuffer> attempt;
    /// The bytes of a section request sliced in place: of a file in memory, sliced on first
    /// poll, or inside the file's tail, sliced as it is inserted.
    private volatile ByteBuffer inPlace;

    SectionRequest(FetchSequence sequence, InputFile inputFile, FetchSequence.Span span, int fileIndex,
                   int workItemIndex, ByteBuffer inPlace) {
        super(fileIndex, workItemIndex, span.start(), span.length(), true);
        this.inPlace = inPlace;
        this.sequence = sequence;
        this.inputFile = inputFile;
        this.reason = span.reason();
        this.holders = span.holders();
    }

    public long offset() {
        return offset;
    }

    int length() {
        return length;
    }

    public long end() {
        return offset + length;
    }

    int workItemIndex() {
        return workItemIndex;
    }

    String reason() {
        return reason;
    }

    @Override
    Runnable begin() {
        return sequence.begin(this, true)::submit;
    }

    @Override
    void dropped() {
        attempt = null;
    }

    /// The section request's bytes, or `null` while they are not present, without waiting and
    /// without issuing a read, except for a file whose bytes are in memory, which it slices in
    /// place.
    ///
    /// @throws IOException if the demand read failed
    public ByteBuffer poll() throws IOException {
        if (sequence == null) {
            ByteBuffer bytes = inPlace;
            if (bytes == null) {
                // A race slices twice, which costs nothing on a file in memory
                bytes = ReadRanges.read(inputFile, offset, length);
                inPlace = bytes;
            }
            return bytes;
        }
        RequestAttempt<ByteBuffer> current = attempt;
        if (current == null || !current.isDone() || current.failedAhead()) {
            return null;
        }
        return current.take();
    }

    /// Reads the section request's bytes from the file, under the section request's
    /// [FetchReason].
    ByteBuffer readBytes() throws IOException {
        try (FetchReason.Scope ignored = FetchReason.set(reason)) {
            return ReadRanges.read(inputFile, offset, length);
        }
    }

    /// A slice of `length` bytes at absolute `fileOffset` from bytes [#poll] returned.
    ///
    /// @throws IllegalArgumentException if the range lies outside the section request
    public ByteBuffer slice(ByteBuffer polled, long fileOffset, int sliceLength) {
        if (fileOffset < offset || sliceLength < 0 || fileOffset + sliceLength > end()) {
            throw new IllegalArgumentException("Range [" + fileOffset + ", " + (fileOffset + sliceLength)
                    + ") lies outside the section request [" + offset + ", " + end() + ") (" + reason + ")");
        }
        return polled.slice(Math.toIntExact(fileOffset - offset), sliceLength);
    }

    @Override
    public RequestAttempt<ByteBuffer> issue() {
        if (sequence == null) {
            throw new IllegalStateException("A section request sliced in place is never read");
        }
        return sequence.demand(this);
    }

    /// Records that a column holding the section request has passed it.
    public void release() {
        if (sequence != null) {
            sequence.release(this);
        }
    }

    /// Releases the section request whatever its holders, as the read closes.
    public void discard() {
        if (sequence != null) {
            sequence.discard(this);
        }
    }

    @Override
    public String toString() {
        return "SectionRequest[" + reason + " at " + offset + " (length " + length + ")]";
    }
}
