/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression.lz4;

import java.nio.ByteBuffer;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.reader.ParquetReadException;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

/// Decompressor for LZ4_RAW compressed data (standard LZ4 block format).
///
/// This is used for the LZ4_RAW codec which uses standard LZ4 block compression
/// without any framing or headers.
///
/// The safe decompressor bounds its reads by the page and its writes by the declared size. The
/// fast one stops only once it has written the declared size, and the native implementation
/// some lz4-java releases give it reads past the page wherever the header overstates it.
public class Lz4RawDecompressor implements Decompressor {

    private final LZ4SafeDecompressor decompressor;

    public Lz4RawDecompressor() {
        this.decompressor = LZ4Factory.fastestInstance().safeDecompressor();
    }

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize, byte[] dest, DecodeBuffers buffers) {
        int actualSize;
        try {
            actualSize = decompressor.decompress(compressed, compressed.position(), compressed.remaining(),
                    ByteBuffer.wrap(dest), 0, uncompressedSize);
        }
        catch (RuntimeException e) {
            // An LZ4Exception's message gives an offset into the buffer's backing array, which
            // differs with where the page lies rather than with the page; some lz4-java releases
            // raise IndexOutOfBoundsException instead
            throw new ParquetReadException("LZ4_RAW decompression failed: malformed block", e);
        }

        if (actualSize != uncompressedSize) {
            throw new ParquetReadException(
                    "LZ4_RAW decompression size mismatch: expected " + uncompressedSize + ", got " + actualSize);
        }

        return dest;
    }

    @Override
    public String getName() {
        return "LZ4_RAW";
    }
}
