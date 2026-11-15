/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import dev.hardwood.metadata.CompressionCodec;

/// The compression levels each codec the writer produces accepts.
///
/// The ranges are fixed here rather than asked of the codec libraries, so that a level is
/// validated without loading a native library, or with the library absent: a missing library
/// fails when the writer is created, as it does for a codec without a level.
public final class CompressionLevels {

    /// zstd's `ZSTD_minCLevel()`: the fastest of its negative levels.
    static final int ZSTD_MIN = -(1 << 17);
    /// zstd's `ZSTD_maxCLevel()`.
    static final int ZSTD_MAX = 22;
    static final int GZIP_MIN = 0;
    static final int GZIP_MAX = 9;
    static final int BROTLI_MIN = 0;
    static final int BROTLI_MAX = 11;
    /// lz4-java's `LZ4Constants.MAX_COMPRESSION_LEVEL` for its high compressor (LZ4 HC).
    static final int LZ4_RAW_MIN = 1;
    static final int LZ4_RAW_MAX = 17;

    private CompressionLevels() {
    }

    /// @throws IllegalArgumentException if `level` is outside the range of `codec`
    public static void requireValid(CompressionCodec codec, int level) {
        switch (codec) {
            case ZSTD -> requireWithin(codec, level, ZSTD_MIN, ZSTD_MAX);
            case GZIP -> requireWithin(codec, level, GZIP_MIN, GZIP_MAX);
            case BROTLI -> requireWithin(codec, level, BROTLI_MIN, BROTLI_MAX);
            case LZ4_RAW -> requireWithin(codec, level, LZ4_RAW_MIN, LZ4_RAW_MAX);
            default -> throw new IllegalStateException("Codec " + codec + " has no compression levels");
        }
    }

    private static void requireWithin(CompressionCodec codec, int level, int min, int max) {
        if (level < min || level > max) {
            throw new IllegalArgumentException("Compression level " + level + " is outside " + codec + "'s range "
                    + min + " to " + max);
        }
    }
}
