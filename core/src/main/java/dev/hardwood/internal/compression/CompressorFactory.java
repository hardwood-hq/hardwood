/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.util.OptionalInt;

import dev.hardwood.internal.compression.CodecLibraries.Library;
import dev.hardwood.internal.compression.lz4.Lz4RawCompressor;
import dev.hardwood.metadata.CompressionCodec;

/// Factory for [Compressor] instances, the write-side counterpart to [DecompressorFactory].
///
/// Every codec is either produced or refused for a reason of its own. `LZ4` and `LZO` are the
/// two refusals, and neither is waiting on a later increment: `LZ4` names the Hadoop framing the
/// format deprecated in favour of `LZ4_RAW`, and `LZO` has no maintained JVM implementation to
/// call. The read path decompresses `LZ4` all the same, files written before the deprecation
/// existing, and refuses `LZO` for the reason this side does.
public class CompressorFactory {

    /// Get a compressor for the given compression codec.
    ///
    /// @param codec the compression codec
    /// @return the appropriate compressor
    /// @throws UnsupportedOperationException if the codec is one this writer does not produce,
    ///         or the required library is missing
    public Compressor getCompressor(CompressionCodec codec) {
        return getCompressor(codec, OptionalInt.empty());
    }

    /// Get a compressor for the given compression codec, compressing at `level` where present.
    ///
    /// @param codec the compression codec
    /// @param level the level, or empty for the codec library's default; validated by
    ///        [CompressionLevels#requireValid] when the configuration was built
    /// @return the appropriate compressor
    /// @throws UnsupportedOperationException if the codec is one this writer does not produce,
    ///         or the required library is missing
    public Compressor getCompressor(CompressionCodec codec, OptionalInt level) {
        return switch (codec) {
            case UNCOMPRESSED -> new UncompressedCompressor();
            case GZIP -> level.isPresent() ? new GzipCompressor(level.getAsInt()) : new GzipCompressor();
            case SNAPPY -> {
                CodecLibraries.require(Library.SNAPPY, "SNAPPY", "write");
                yield new SnappyCompressor();
            }
            case ZSTD -> {
                CodecLibraries.require(Library.ZSTD, "ZSTD", "write");
                yield level.isPresent() ? new ZstdCompressor(level.getAsInt()) : new ZstdCompressor();
            }
            case LZ4_RAW -> {
                CodecLibraries.require(Library.LZ4, "LZ4_RAW", "write");
                yield level.isPresent() ? new Lz4RawCompressor(level.getAsInt()) : new Lz4RawCompressor();
            }
            case BROTLI -> {
                CodecLibraries.require(Library.BROTLI, "BROTLI", "write");
                yield level.isPresent() ? new BrotliCompressor(level.getAsInt()) : new BrotliCompressor();
            }
            case LZ4, LZO -> throw refusal(codec);
        };
    }

    /// Why `codec`, `LZ4` or `LZO`, is not written.
    public static UnsupportedOperationException refusal(CompressionCodec codec) {
        return switch (codec) {
            case LZ4 -> new UnsupportedOperationException(
                    "LZ4 uses the Hadoop framing the Parquet format has deprecated, so it is not written; "
                            + "use LZ4_RAW instead. Files already written with LZ4 are still read.");
            case LZO -> new UnsupportedOperationException(
                    "LZO compression is not supported: there is no maintained JVM implementation under a "
                            + "licence this project can depend on. The read path refuses it for the same reason.");
            default -> throw new IllegalArgumentException(codec + " is written");
        };
    }
}
