/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.util.Arrays;

import com.github.luben.zstd.Zstd;

import dev.hardwood.writer.ParquetWriteException;

/// [Compressor] for the ZSTD codec, the inverse of [ZstdDecompressor]. Compresses at zstd's
/// default level, the same trade-off point the reference implementations write, unless given
/// another.
public class ZstdCompressor implements Compressor {

    private final int level;

    /// Compresses at zstd's default level.
    public ZstdCompressor() {
        this(Zstd.defaultCompressionLevel());
    }

    /// @param level a level within [CompressionLevels]' range for `ZSTD`
    public ZstdCompressor(int level) {
        this.level = level;
    }

    @Override
    public byte[] compress(byte[] data, int offset, int length) {
        byte[] output = new byte[Math.toIntExact(Zstd.compressBound(length))];
        long written;
        try {
            written = Zstd.compressByteArray(output, 0, output.length, data, offset, length, level);
        }
        catch (RuntimeException e) {
            // zstd-jni reports some failures by throwing rather than through the return code
            // the check below covers, the same way it does on the decompress side.
            throw new ParquetWriteException("ZSTD compression failed: " + e.getMessage(), e);
        }
        if (Zstd.isError(written)) {
            throw new ParquetWriteException(
                    "ZSTD compression failed: " + Zstd.getErrorName(written));
        }
        return Arrays.copyOf(output, Math.toIntExact(written));
    }

    @Override
    public String getName() {
        return "ZSTD";
    }
}
