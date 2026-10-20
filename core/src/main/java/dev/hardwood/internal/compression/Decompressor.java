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
import dev.hardwood.reader.ParquetReadException;

/// Interface for decompressing compressed page data.
///
/// A decompressor reads `compressed` from its position to its limit and leaves the position and
/// the limit as they were, whether the buffer is on the heap or direct, so the same buffer
/// decompresses again.
public interface Decompressor {

    /// Decompresses `compressed` into `dest`, which holds at least `uncompressedSize` bytes and
    /// belongs to the caller. A stream that decodes to `uncompressedSize` bytes writes nothing past
    /// them.
    ///
    /// @param buffers the decoding task's buffers, which hold a codec's scratch
    /// @return `dest`
    /// @throws ParquetReadException if the bytes do not decode to exactly `uncompressedSize` bytes
    byte[] decompress(ByteBuffer compressed, int uncompressedSize, byte[] dest, DecodeBuffers buffers);

    /// Get the name of this decompressor.
    String getName();
}
