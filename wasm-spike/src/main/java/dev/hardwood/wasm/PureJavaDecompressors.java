/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.wasm;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Supplier;

import org.brotli.dec.BrotliInputStream;

import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.metadata.CompressionCodec;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.zstd.ZstdDecompressor;

/// Pure-Java [Decompressor] implementations for the codecs Hardwood normally
/// handles through JNI libraries (snappy-java, zstd-jni, lz4-java, brotli4j).
///
/// These back a JNI-free read path so the reader can run on a GraalVM Web Image
/// (WebAssembly) build, where no native libraries can be loaded. `UNCOMPRESSED`
/// and `GZIP` already have pure-Java built-ins (`java.util.zip.Inflater`), so they
/// are not overridden here.
///
/// `SNAPPY` uses a self-contained decoder ([SnappyBlock]) with no `sun.misc.Unsafe`, so it
/// runs under Web Image. `ZSTD`/`LZ4_RAW` still delegate to aircompressor, which relies on
/// `sun.misc.Unsafe` and therefore does not yet work in a Web Image build. Only the modern
/// `LZ4_RAW` block format is provided; the legacy Hadoop-framed `LZ4` codec is not covered.
public final class PureJavaDecompressors {

    private PureJavaDecompressors() {
    }

    /// The per-codec override map to pass to
    /// `HardwoodContextImpl.create(int, Map)`.
    public static Map<CompressionCodec, Supplier<Decompressor>> overrides() {
        Map<CompressionCodec, Supplier<Decompressor>> map = new EnumMap<>(CompressionCodec.class);
        map.put(CompressionCodec.SNAPPY, Snappy::new);
        map.put(CompressionCodec.GZIP, Gzip::new);
        map.put(CompressionCodec.ZSTD, Zstd::new);
        map.put(CompressionCodec.LZ4_RAW, Lz4Raw::new);
        map.put(CompressionCodec.BROTLI, Brotli::new);
        return map;
    }

    private static byte[] remainingBytes(ByteBuffer compressed) {
        ByteBuffer dup = compressed.duplicate();
        byte[] bytes = new byte[dup.remaining()];
        dup.get(bytes);
        return bytes;
    }

    private static byte[] checkSize(byte[] output, int produced, int expected, String codec) throws IOException {
        if (produced != expected) {
            throw new IOException(codec + " decompression size mismatch: expected " + expected + ", got " + produced);
        }
        return output;
    }

    /// Snappy raw block format (matches Parquet SNAPPY), decoded with [SnappyBlock] — no
    /// `sun.misc.Unsafe`, so it works under GraalVM Web Image.
    static final class Snappy implements Decompressor {

        @Override
        public byte[] decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
            byte[] output = new byte[uncompressedSize];
            int produced = SnappyBlock.decompress(remainingBytes(compressed), output);
            return checkSize(output, produced, uncompressedSize, getName());
        }

        @Override
        public String getName() {
            return "SNAPPY";
        }
    }

    /// GZIP via jzlib (pure-Java zlib, no native `Inflater.init`).
    static final class Gzip implements Decompressor {

        @Override
        public byte[] decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
            byte[] output = new byte[uncompressedSize];
            int produced = GzipInflate.decompress(remainingBytes(compressed), output);
            return checkSize(output, produced, uncompressedSize, getName());
        }

        @Override
        public String getName() {
            return "GZIP";
        }
    }

    /// Zstandard.
    static final class Zstd implements Decompressor {

        private final ZstdDecompressor delegate = new ZstdDecompressor();

        @Override
        public byte[] decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
            byte[] input = remainingBytes(compressed);
            byte[] output = new byte[uncompressedSize];
            int produced = delegate.decompress(input, 0, input.length, output, 0, uncompressedSize);
            return checkSize(output, produced, uncompressedSize, getName());
        }

        @Override
        public String getName() {
            return "ZSTD";
        }
    }

    /// LZ4 raw block format (matches Parquet LZ4_RAW).
    static final class Lz4Raw implements Decompressor {

        private final Lz4Decompressor delegate = new Lz4Decompressor();

        @Override
        public byte[] decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
            byte[] input = remainingBytes(compressed);
            byte[] output = new byte[uncompressedSize];
            int produced = delegate.decompress(input, 0, input.length, output, 0, uncompressedSize);
            return checkSize(output, produced, uncompressedSize, getName());
        }

        @Override
        public String getName() {
            return "LZ4_RAW";
        }
    }

    /// Brotli, via the pure-Java `org.brotli.dec` decoder.
    static final class Brotli implements Decompressor {

        @Override
        public byte[] decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
            byte[] input = remainingBytes(compressed);
            byte[] output = new byte[uncompressedSize];
            try (BrotliInputStream stream = new BrotliInputStream(new ByteArrayInputStream(input))) {
                int offset = 0;
                while (offset < uncompressedSize) {
                    int read = stream.read(output, offset, uncompressedSize - offset);
                    if (read < 0) {
                        break;
                    }
                    offset += read;
                }
                return checkSize(output, offset, uncompressedSize, getName());
            }
        }

        @Override
        public String getName() {
            return "BROTLI";
        }
    }
}
