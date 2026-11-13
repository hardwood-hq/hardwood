/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.nio.ByteBuffer;

import dev.hardwood.internal.DecodeBuffers;

/// The heap copy of a compressed page for a codec that reads only a `byte[]`, in a buffer the
/// task's [DecodeBuffers] keep. A local file's pages are memory-mapped and a remote file's are
/// direct, so such a codec copies every page; a new array per page would be a humongous one for
/// a page above half of G1's smallest region.
public final class HeapCopies {

    private static final DecodeBuffers.Slot<byte[]> COMPRESSED_BYTES = DecodeBuffers.slot();

    private HeapCopies() {
    }

    /// `compressed`'s remaining bytes at the start of a buffer of at least that length, valid until
    /// the next call with `buffers`, leaving `compressed`'s position where it was. A page above
    /// [DecodeBuffers#RETAINED_BYTES] gets a buffer of its own.
    public static byte[] copy(ByteBuffer compressed, DecodeBuffers buffers) {
        int length = compressed.remaining();
        byte[] bytes;
        if (length > DecodeBuffers.RETAINED_BYTES) {
            bytes = new byte[length];
        }
        else {
            bytes = buffers.get(COMPRESSED_BYTES);
            if (bytes == null || bytes.length < length) {
                bytes = new byte[length];
                buffers.set(COMPRESSED_BYTES, bytes);
            }
        }
        compressed.get(compressed.position(), bytes, 0, length);
        return bytes;
    }
}
