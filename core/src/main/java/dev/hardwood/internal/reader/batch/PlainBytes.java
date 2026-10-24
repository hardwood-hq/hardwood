/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

/// Where one fill copies its plain values' bytes: chunks of the batch of at most
/// [BinaryBatchValues#CHUNK_BYTES], each added to the batch's buffers as the fill starts it. A value
/// never spans two chunks; a value longer than a chunk gets one of its own length. A chunk is sized
/// to the fill's bytes not yet placed where those are fewer, so the last one is not oversized. A
/// batch that keeps its chunks ([BinaryBatchValues#keepChunks]) takes a full chunk, which its next
/// fill can reuse, where at least half a chunk's bytes remain, so a full chunk is at most half
/// empty and a fill of few bytes takes no more than them.
public final class PlainBytes {

    private static final byte[] EMPTY = new byte[0];

    private final BinaryBatchValues out;
    /// The fill's bytes not yet placed.
    private long remaining;
    private byte[] chunk;
    private int id;
    private int pos;

    /// @param bytes the bytes of every plain value the fill copies
    public PlainBytes(BinaryBatchValues out, long bytes) {
        this.out = out;
        this.remaining = bytes;
    }

    /// Readies room for a value of `length` bytes, starting a chunk where the current one lacks it,
    /// and returns the chunk it goes to, at [#position()].
    ///
    /// @throws IllegalStateException if the fill places more bytes than it was given
    public byte[] room(int length) {
        if (chunk == null && length == 0) {
            // An empty value before any chunk points into the batch's empty buffer
            id = 0;
            return EMPTY;
        }
        if (chunk == null || chunk.length - pos < length) {
            if (length > remaining) {
                throw new IllegalStateException("A fill places a value of " + length + " bytes with only "
                        + remaining + " of its bytes left");
            }
            if (out.keepsChunks() && length <= BinaryBatchValues.CHUNK_BYTES
                    && remaining >= BinaryBatchValues.CHUNK_BYTES / 2) {
                // A recycled holder's chunks are full, so that its next fill can reuse them
                id = out.takeChunk();
                chunk = out.chunk(id);
            }
            else {
                chunk = new byte[(int) Math.max(length, Math.min(BinaryBatchValues.CHUNK_BYTES, remaining))];
                id = out.addBuffer(chunk);
            }
            pos = 0;
        }
        return chunk;
    }

    /// Where the next value goes in the chunk [#room(int)] returned.
    public int position() {
        return pos;
    }

    /// The batch's buffer id of the chunk [#room(int)] returned.
    public int id() {
        return id;
    }

    /// The bytes the chunk [#room(int)] returned has left from [#position()].
    public int free() {
        return chunk.length - pos;
    }

    /// Records `length` bytes written at [#position()].
    public void advance(int length) {
        pos += length;
        remaining -= length;
    }
}
