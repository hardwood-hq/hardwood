/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import dev.hardwood.metadata.CompressionCodec;

/// Pages in Hadoop's LZ4 framing, which Hardwood reads but does not write: each block of
/// `blockSize` input bytes compressed as raw LZ4 behind its big-endian uncompressed and compressed
/// lengths.
final class Lz4HadoopFrames {

    private Lz4HadoopFrames() {
    }

    static byte[] frame(byte[] body, int blockSize) {
        Compressor compressor = new CompressorFactory().getCompressor(CompressionCodec.LZ4_RAW);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int start = 0; start < body.length; start += blockSize) {
            byte[] block = Arrays.copyOfRange(body, start, Math.min(body.length, start + blockSize));
            byte[] compressed = compressor.compress(block, 0, block.length);
            out.writeBytes(ByteBuffer.allocate(8).putInt(block.length).putInt(compressed.length).array());
            out.writeBytes(compressed);
        }
        return out.toByteArray();
    }
}
