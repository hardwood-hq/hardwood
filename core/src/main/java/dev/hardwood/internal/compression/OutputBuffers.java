/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

/// The per-thread output buffers a [Decompressor] returns from its plain
/// [Decompressor#decompress(java.nio.ByteBuffer, int)] call. Each codec keeps its own
/// `ThreadLocal`, grown to the largest page the thread has decompressed.
public final class OutputBuffers {

    private OutputBuffers() {
    }

    /// The thread's buffer in `buffers`, replaced by a new one if it holds fewer than `minSize` bytes.
    public static byte[] borrow(ThreadLocal<byte[]> buffers, int minSize) {
        byte[] buf = buffers.get();
        if (buf == null || buf.length < minSize) {
            buf = new byte[minSize];
            buffers.set(buf);
        }
        return buf;
    }
}
