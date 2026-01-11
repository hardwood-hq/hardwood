/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.compression;

import dev.morling.hardwood.metadata.CompressionCodec;

/**
 * Factory for creating decompressor instances based on compression codec.
 */
public class DecompressorFactory {

    /**
     * Get a decompressor for the given compression codec.
     *
     * @param codec the compression codec
     * @return the appropriate decompressor
     * @throws UnsupportedOperationException if the codec is not supported
     */
    public static Decompressor getDecompressor(CompressionCodec codec) {
        return switch (codec) {
            case UNCOMPRESSED -> new UncompressedDecompressor();
            case SNAPPY -> new SnappyDecompressor();
            case GZIP -> new GzipDecompressor();
            case ZSTD -> new ZstdDecompressor();
            case LZ4 -> new Lz4Decompressor();
            case LZ4_RAW -> new Lz4RawDecompressor();
            case BROTLI -> new BrotliDecompressor();
            case LZO -> throw new UnsupportedOperationException("LZO compression not yet supported");
        };
    }
}
