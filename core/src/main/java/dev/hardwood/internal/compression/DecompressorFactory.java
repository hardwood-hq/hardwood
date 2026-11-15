/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import dev.hardwood.internal.compression.CodecLibraries.Library;
import dev.hardwood.internal.compression.libdeflate.LibdeflateDecompressor;
import dev.hardwood.internal.compression.libdeflate.LibdeflatePool;
import dev.hardwood.internal.compression.lz4.Lz4Decompressor;
import dev.hardwood.internal.compression.lz4.Lz4RawDecompressor;
import dev.hardwood.metadata.CompressionCodec;

/// Factory for creating decompressor instances based on compression codec.
public class DecompressorFactory {

    private final LibdeflatePool libdeflatePool;

    /// Create a new factory with the given libdeflate pool.
    ///
    /// @param libdeflatePool pool for libdeflate decompressor handles
    public DecompressorFactory(LibdeflatePool libdeflatePool) {
        this.libdeflatePool = libdeflatePool;
    }

    /// Get a decompressor for the given compression codec.
    ///
    /// @param codec the compression codec
    /// @return the appropriate decompressor
    /// @throws UnsupportedOperationException if the codec is not supported or the required library is missing
    public Decompressor getDecompressor(CompressionCodec codec) {
        return switch (codec) {
            case UNCOMPRESSED -> new UncompressedDecompressor();
            case GZIP -> {
                if (libdeflatePool != null) {
                    yield new LibdeflateDecompressor(libdeflatePool);
                }
                yield new GzipDecompressor();
            }
            case SNAPPY -> {
                CodecLibraries.require(Library.SNAPPY, "SNAPPY", "read");
                yield new SnappyDecompressor();
            }
            case ZSTD -> {
                CodecLibraries.require(Library.ZSTD, "ZSTD", "read");
                yield new ZstdDecompressor();
            }
            case LZ4 -> {
                CodecLibraries.require(Library.LZ4, "LZ4", "read");
                yield new Lz4Decompressor();
            }
            case LZ4_RAW -> {
                CodecLibraries.require(Library.LZ4, "LZ4_RAW", "read");
                yield new Lz4RawDecompressor();
            }
            case BROTLI -> {
                CodecLibraries.require(Library.BROTLI, "BROTLI", "read");
                yield new BrotliDecompressor();
            }
            case LZO -> throw new UnsupportedOperationException("LZO compression is not supported");
        };
    }
}
