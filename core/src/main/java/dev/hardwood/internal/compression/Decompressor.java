/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.nio.ByteBuffer;

import dev.hardwood.reader.ParquetReadException;

/// Interface for decompressing compressed page data.
public interface Decompressor {

    /// Decompress the given compressed data from a buffer.
    ///
    /// The returned array contains valid decompressed data in positions `[0, uncompressedSize)`.
    /// The array length may be greater than `uncompressedSize` due to internal buffer reuse;
    /// callers must use `uncompressedSize` as the effective length, not `array.length`.
    ///
    /// @param compressed the buffer slice containing compressed data
    /// @param uncompressedSize the expected size of uncompressed data
    /// @return an array containing the uncompressed data in the first `uncompressedSize` bytes
    /// @throws ParquetReadException if the bytes do not decode
    byte[] decompress(ByteBuffer compressed, int uncompressedSize);

    /// Decompresses into `dest`, which holds at least `uncompressedSize` bytes and belongs to the
    /// caller, so that the bytes outlive the decompressor's next call. A stream that decodes to
    /// `uncompressedSize` bytes writes nothing past them.
    ///
    /// @return `dest`
    /// @throws ParquetReadException if the bytes do not decode to exactly `uncompressedSize` bytes
    default byte[] decompress(ByteBuffer compressed, int uncompressedSize, byte[] dest) {
        System.arraycopy(decompress(compressed, uncompressedSize), 0, dest, 0, uncompressedSize);
        return dest;
    }

    /// Get the name of this decompressor.
    String getName();
}
