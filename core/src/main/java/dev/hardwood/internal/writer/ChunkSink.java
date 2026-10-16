/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import dev.hardwood.OutputFile;

/// Where an encoded column chunk's bytes go as [ColumnChunkBuffer#encodeTo] produces them: the
/// file itself, for the chunk next in schema order, or a [Buffered] chunk held until every chunk
/// ahead of it has been written.
interface ChunkSink {

    /// Takes `length` bytes of `bytes` from `offset`.
    ///
    /// @param reused whether the caller overwrites `bytes` once this returns, so a sink that keeps
    ///        them must copy them
    void write(byte[] bytes, int offset, int length, boolean reused) throws IOException;

    /// A sink writing straight to `out`, which copies what it keeps of each write.
    static ChunkSink to(OutputFile out) {
        return (bytes, offset, length, reused) -> out.write(ByteBuffer.wrap(bytes, offset, length));
    }

    /// An encoded chunk held in memory, written out by [#writeTo] in the slices it arrived in, so
    /// the destination sees the same writes as from a chunk streamed to it.
    final class Buffered implements ChunkSink {

        private final List<ByteBuffer> slices = new ArrayList<>();

        @Override
        public void write(byte[] bytes, int offset, int length, boolean reused) {
            slices.add(reused
                    ? ByteBuffer.wrap(Arrays.copyOfRange(bytes, offset, offset + length))
                    : ByteBuffer.wrap(bytes, offset, length));
        }

        /// Writes the chunk to `out` and lets go of it.
        void writeTo(OutputFile out) throws IOException {
            for (ByteBuffer slice : slices) {
                out.write(slice);
            }
            slices.clear();
        }
    }
}
