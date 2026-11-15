/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import dev.hardwood.writer.ParquetWriteException;

/// [Compressor] for the GZIP codec, the inverse of [GzipDecompressor]. Backed by the JDK's
/// `Deflater`, so it adds no dependency to the classpath.
///
/// The body is written in the gzip framing — the 10-byte header, the deflate stream and the
/// CRC / length trailer — which is what Parquet's GZIP codec is and what [GzipDecompressor]
/// reads. Deflate compresses at its default level unless given another.
public class GzipCompressor implements Compressor {

    /// The deflate buffer `GZIPOutputStream` works through. Its default is 512 bytes, which
    /// would cross the native boundary far more often than necessary for a page-sized body.
    private static final int DEFLATE_BUFFER_BYTES = 8192;

    private final int level;

    /// Compresses at deflate's default level.
    public GzipCompressor() {
        this(Deflater.DEFAULT_COMPRESSION);
    }

    /// @param level a level within [CompressionLevels]' range for `GZIP`, or
    ///        [Deflater#DEFAULT_COMPRESSION]
    public GzipCompressor(int level) {
        this.level = level;
    }

    @Override
    public byte[] compress(byte[] data, int offset, int length) {
        // A page body that compresses at all lands well under its uncompressed size, and the
        // stream grows the buffer for the rare body that does not.
        ByteArrayOutputStream compressed = new ByteArrayOutputStream(Math.max(64, length / 2));
        try (GZIPOutputStream gzip = new LeveledGzipOutputStream(compressed, DEFLATE_BUFFER_BYTES, level)) {
            gzip.write(data, offset, length);
        }
        catch (IOException e) {
            // The sink is a byte array, so this is deflate refusing the body.
            throw new ParquetWriteException("GZIP compression failed", e);
        }
        return compressed.toByteArray();
    }

    @Override
    public String getName() {
        return "GZIP";
    }

    /// `GZIPOutputStream` takes no level; its `Deflater` is reachable only from a subclass.
    private static final class LeveledGzipOutputStream extends GZIPOutputStream {

        LeveledGzipOutputStream(ByteArrayOutputStream out, int bufferBytes, int level) throws IOException {
            super(out, bufferBytes);
            def.setLevel(level);
        }
    }
}
