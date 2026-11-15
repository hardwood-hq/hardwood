/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.util.Objects;
import java.util.OptionalInt;

import dev.hardwood.internal.compression.CompressionLevels;
import dev.hardwood.internal.compression.CompressorFactory;
import dev.hardwood.metadata.CompressionCodec;

/// How [ParquetFileWriter] compresses each page body: a codec, and for the codecs that have
/// them, the level it compresses at.
///
/// Obtain one from the factory for its codec, and set a level on the codecs that take one:
///
/// ```java
/// WriterConfig config = WriterConfig.builder()
///         .compression(Compression.zstd().level(9))
///         .build();
/// ```
///
/// | Factory | Codec | Levels | Default level |
/// |---------|-------|--------|---------------|
/// | [#uncompressed()] | `UNCOMPRESSED` | — | — |
/// | [#snappy()] | `SNAPPY` | — | — |
/// | [#gzip()] | `GZIP` | 0 (stored, not compressed) to 9 | 6 |
/// | [#zstd()] | `ZSTD` | -131072 to 22; 0 selects the default | 3 |
/// | [#lz4Raw()] | `LZ4_RAW` | 1 to 17, through LZ4's high compressor (LZ4 HC) | LZ4's fast compressor |
/// | [#brotli()] | `BROTLI` | 0 to 11 | 11 |
///
/// A higher level writes a smaller file and takes longer to write; reading the file takes about
/// as long at any level. Every codec but `UNCOMPRESSED` and `GZIP` needs its library on the
/// classpath, which is checked when the writer is created.
///
/// Instances are immutable; equal when their codec and level are.
public sealed interface Compression permits Compression.Fixed, Compression.Leveled {

    /// The codec page bodies are compressed with.
    CompressionCodec codec();

    /// The level the codec compresses at, or empty for its default.
    OptionalInt level();

    /// Page bodies stored as they are.
    static Compression uncompressed() {
        return Fixed.UNCOMPRESSED;
    }

    /// Snappy, which has no levels.
    static Compression snappy() {
        return Fixed.SNAPPY;
    }

    /// GZIP at deflate's default level; see [Leveled#level(int)] for another.
    static Leveled gzip() {
        return Leveled.GZIP;
    }

    /// ZSTD at zstd's default level; see [Leveled#level(int)] for another.
    static Leveled zstd() {
        return Leveled.ZSTD;
    }

    /// LZ4_RAW through LZ4's fast compressor; see [Leveled#level(int)] for the high compressor.
    static Leveled lz4Raw() {
        return Leveled.LZ4_RAW;
    }

    /// Brotli at its default quality; see [Leveled#level(int)] for another.
    static Leveled brotli() {
        return Leveled.BROTLI;
    }

    /// The codec at its default level, for a codec chosen at runtime, such as the codec of a
    /// file being copied.
    ///
    /// @param codec the codec; must be non-null
    /// @return the compression
    /// @throws IllegalArgumentException if `codec` is `null`
    /// @throws UnsupportedOperationException if `codec` is `LZ4`, whose Hadoop framing the format
    ///         has deprecated in favour of `LZ4_RAW`, or `LZO`, which has no maintained JVM
    ///         implementation; neither is written
    static Compression of(CompressionCodec codec) {
        if (codec == null) {
            throw new IllegalArgumentException("codec must not be null");
        }
        return switch (codec) {
            case UNCOMPRESSED -> uncompressed();
            case SNAPPY -> snappy();
            case GZIP -> gzip();
            case ZSTD -> zstd();
            case LZ4_RAW -> lz4Raw();
            case BROTLI -> brotli();
            case LZ4, LZO -> throw CompressorFactory.refusal(codec);
        };
    }

    /// A codec without levels.
    final class Fixed implements Compression {

        private static final Fixed UNCOMPRESSED = new Fixed(CompressionCodec.UNCOMPRESSED);
        private static final Fixed SNAPPY = new Fixed(CompressionCodec.SNAPPY);

        private final CompressionCodec codec;

        private Fixed(CompressionCodec codec) {
            this.codec = codec;
        }

        @Override
        public CompressionCodec codec() {
            return codec;
        }

        @Override
        public OptionalInt level() {
            return OptionalInt.empty();
        }

        @Override
        public String toString() {
            return codec.toString();
        }
    }

    /// A codec with levels, at its default level or the one [#level(int)] set.
    final class Leveled implements Compression {

        private static final Leveled GZIP = new Leveled(CompressionCodec.GZIP, OptionalInt.empty());
        private static final Leveled ZSTD = new Leveled(CompressionCodec.ZSTD, OptionalInt.empty());
        private static final Leveled LZ4_RAW = new Leveled(CompressionCodec.LZ4_RAW, OptionalInt.empty());
        private static final Leveled BROTLI = new Leveled(CompressionCodec.BROTLI, OptionalInt.empty());

        private final CompressionCodec codec;
        private final OptionalInt level;

        private Leveled(CompressionCodec codec, OptionalInt level) {
            this.codec = codec;
            this.level = level;
        }

        /// This codec at `level`.
        ///
        /// @param level a level within the codec's range; see [Compression] for the ranges
        /// @return the codec at that level
        /// @throws IllegalArgumentException if `level` is outside the codec's range
        public Leveled level(int level) {
            CompressionLevels.requireValid(codec, level);
            return new Leveled(codec, OptionalInt.of(level));
        }

        @Override
        public CompressionCodec codec() {
            return codec;
        }

        @Override
        public OptionalInt level() {
            return level;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Leveled that && codec == that.codec && level.equals(that.level);
        }

        @Override
        public int hashCode() {
            return Objects.hash(codec, level);
        }

        @Override
        public String toString() {
            return level.isPresent() ? codec + " level " + level.getAsInt() : codec.toString();
        }
    }
}
