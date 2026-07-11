/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.io.IOException;
import java.util.Arrays;

import com.github.luben.zstd.Zstd;

/// [Compressor] for the ZSTD codec, the inverse of [ZstdDecompressor]. Compresses at zstd's
/// default level, the same trade-off point the reference implementations write.
public class ZstdCompressor implements Compressor {

    private final int level = Zstd.defaultCompressionLevel();

    @Override
    public byte[] compress(byte[] data, int offset, int length) throws IOException {
        byte[] output = new byte[Math.toIntExact(Zstd.compressBound(length))];
        long written = Zstd.compressByteArray(output, 0, output.length, data, offset, length, level);
        if (Zstd.isError(written)) {
            throw new IOException("ZSTD compression failed: " + Zstd.getErrorName(written));
        }
        return Arrays.copyOf(output, Math.toIntExact(written));
    }

    @Override
    public String getName() {
        return "ZSTD";
    }
}
