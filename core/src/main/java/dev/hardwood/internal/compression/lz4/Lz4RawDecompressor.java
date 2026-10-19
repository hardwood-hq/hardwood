/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression.lz4;

import java.nio.ByteBuffer;

import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.internal.compression.OutputBuffers;
import dev.hardwood.reader.ParquetReadException;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/// Decompressor for LZ4_RAW compressed data (standard LZ4 block format).
///
/// This is used for the LZ4_RAW codec which uses standard LZ4 block compression
/// without any framing or headers.
public class Lz4RawDecompressor implements Decompressor {

    private static final ThreadLocal<byte[]> OUTPUT_BUFFER = new ThreadLocal<>();

    private final LZ4FastDecompressor decompressor;

    public Lz4RawDecompressor() {
        this.decompressor = LZ4Factory.fastestInstance().fastDecompressor();
    }

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize) {
        return decompress(compressed, uncompressedSize, OutputBuffers.borrow(OUTPUT_BUFFER, uncompressedSize));
    }

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize, byte[] dest) {
        int compressedLength = compressed.remaining();
        int consumedLength;
        try {
            consumedLength = decompressor.decompress(compressed, compressed.position(), ByteBuffer.wrap(dest), 0,
                    uncompressedSize);
        }
        catch (Exception e) {
            throw new ParquetReadException("LZ4_RAW decompression failed: " + e.getMessage(), e);
        }

        // The decompressor bounds its reads by the buffer's capacity, not its limit
        if (consumedLength != compressedLength) {
            throw new ParquetReadException(
                    "LZ4_RAW decompression did not consume all input: expected " + compressedLength +
                            " bytes, consumed " + consumedLength);
        }

        return dest;
    }

    @Override
    public String getName() {
        return "LZ4_RAW";
    }
}
