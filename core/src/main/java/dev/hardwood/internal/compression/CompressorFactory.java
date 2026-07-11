/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import dev.hardwood.metadata.CompressionCodec;

/// Factory for [Compressor] instances, the write-side counterpart to [DecompressorFactory].
///
/// The write path currently supports `UNCOMPRESSED` and `ZSTD`; the remaining codecs are
/// rejected until later increments add their encode side.
public class CompressorFactory {

    /// Get a compressor for the given compression codec.
    ///
    /// @param codec the compression codec
    /// @return the appropriate compressor
    /// @throws UnsupportedOperationException if the codec cannot yet be written, or the required
    ///         library is missing
    public Compressor getCompressor(CompressionCodec codec) {
        return switch (codec) {
            case UNCOMPRESSED -> new UncompressedCompressor();
            case ZSTD -> {
                checkClassAvailable("com.github.luben.zstd.Zstd", "ZSTD", "com.github.luben:zstd-jni");
                yield new ZstdCompressor();
            }
            default -> throw new UnsupportedOperationException(
                    "Writing " + codec + "-compressed pages is not yet supported");
        };
    }

    private static void checkClassAvailable(String className, String codecName, String dependency) {
        try {
            Class.forName(className);
        }
        catch (ClassNotFoundException e) {
            throw new UnsupportedOperationException(
                    "Cannot write " + codecName + "-compressed Parquet file: required library not found. " +
                            "Add the following dependency to your project: " + dependency);
        }
    }
}
