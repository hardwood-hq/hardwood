/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.nio.ByteBuffer;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDecompressCtx;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.reader.ParquetReadException;

/// Decompressor for ZSTD compressed data.
public class ZstdDecompressor implements Decompressor {

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize, byte[] output, DecodeBuffers buffers) {
        int actualSize;
        try {
            actualSize = compressed.hasArray()
                    // Heap to heap, bounded to the declared size
                    ? Math.toIntExact(Zstd.decompressByteArray(output, 0, uncompressedSize, compressed.array(),
                            compressed.arrayOffset() + compressed.position(), compressed.remaining()))
                    : decompressDirect(DirectBuffers.ensureDirect(compressed), uncompressedSize, output);
        }
        catch (RuntimeException e) {
            // zstd-jni reports a frame it cannot read, or one decoding past the bound, by throwing
            // rather than by the return code the size check below covers.
            throw new ParquetReadException("ZSTD decompression failed: " + e.getMessage(), e);
        }

        if (actualSize != uncompressedSize) {
            throw new ParquetReadException(
                    "ZSTD decompression size mismatch: expected " + uncompressedSize + ", got " + actualSize);
        }

        return output;
    }

    /// Direct to heap, bounded to the declared size
    private static int decompressDirect(ByteBuffer compressed, int uncompressedSize, byte[] output) {
        try (ZstdDecompressCtx ctx = new ZstdDecompressCtx()) {
            return ctx.decompressDirectByteBufferToByteArray(output, 0, uncompressedSize, compressed,
                    compressed.position(), compressed.remaining());
        }
    }

    @Override
    public String getName() {
        return "ZSTD";
    }
}
