/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

/**
 * Decompressor for LZ4 compressed data (Hadoop-style format).
 * <p>
 * The Parquet LZ4 codec has historically been ambiguous. This implementation
 * supports the Hadoop native LZ4 format which uses blocks with headers:
 * <p>
 * Format: [4-byte original_len, LE][4-byte compressed_len, LE][compressed data]...
 * <p>
 * If the data doesn't match Hadoop format, falls back to raw LZ4 block decompression.
 */
public class Lz4Decompressor implements Decompressor {

    private final LZ4FastDecompressor fastDecompressor;
    private final LZ4SafeDecompressor safeDecompressor;

    public Lz4Decompressor() {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        this.fastDecompressor = factory.fastDecompressor();
        this.safeDecompressor = factory.safeDecompressor();
    }

    @Override
    public byte[] decompress(MappedByteBuffer compressed, int uncompressedSize) throws IOException {
        // Try raw LZ4 first (most common case), then fall back to Hadoop format
        try {
            return decompressRaw(compressed, uncompressedSize);
        }
        catch (Exception e) {
            // Fall back to Hadoop's block format - needs byte[] for block parsing
            try {
                byte[] compressedBytes = new byte[compressed.remaining()];
                compressed.duplicate().get(compressedBytes);
                return decompressHadoopFormat(compressedBytes, uncompressedSize);
            }
            catch (Exception e2) {
                throw new IOException("LZ4 decompression failed (tried both raw and Hadoop formats): " +
                        e.getMessage(), e);
            }
        }
    }

    /**
     * Decompress using Hadoop's native LZ4 block format.
     * <p>
     * Format: [4-byte uncompressed_len, BE][4-byte compressed_len, BE][compressed data]...
     * <p>
     * Each block has both the uncompressed and compressed sizes in big-endian format,
     * followed by the compressed data.
     */
    private byte[] decompressHadoopFormat(byte[] compressed, int uncompressedSize) throws IOException {
        byte[] uncompressed = new byte[uncompressedSize];
        int srcOffset = 0;
        int destOffset = 0;

        ByteBuffer buffer = ByteBuffer.wrap(compressed).order(ByteOrder.BIG_ENDIAN);

        while (destOffset < uncompressedSize && srcOffset < compressed.length) {
            if (srcOffset + 8 > compressed.length) {
                throw new IOException("Truncated LZ4 Hadoop block header");
            }

            // Read uncompressed block size (4 bytes, big-endian)
            int blockUncompressedSize = buffer.getInt(srcOffset);
            srcOffset += 4;

            // Read compressed block size (4 bytes, big-endian)
            int blockCompressedSize = buffer.getInt(srcOffset);
            srcOffset += 4;

            if (blockUncompressedSize == 0) {
                continue;
            }

            if (blockUncompressedSize < 0 || blockCompressedSize < 0) {
                throw new IOException("Invalid LZ4 Hadoop block sizes: uncompressed=" +
                        blockUncompressedSize + ", compressed=" + blockCompressedSize);
            }

            if (srcOffset + blockCompressedSize > compressed.length) {
                throw new IOException("LZ4 compressed block extends beyond buffer");
            }

            if (blockCompressedSize == blockUncompressedSize) {
                // Data stored uncompressed (compression didn't help)
                System.arraycopy(compressed, srcOffset, uncompressed, destOffset, blockUncompressedSize);
                destOffset += blockUncompressedSize;
            }
            else {
                // Decompress this block
                int decompressedLen = safeDecompressor.decompress(
                        compressed, srcOffset, blockCompressedSize,
                        uncompressed, destOffset, blockUncompressedSize);

                if (decompressedLen != blockUncompressedSize) {
                    throw new IOException("LZ4 block size mismatch: expected " +
                            blockUncompressedSize + ", got " + decompressedLen);
                }
                destOffset += decompressedLen;
            }

            srcOffset += blockCompressedSize;
        }

        if (destOffset != uncompressedSize) {
            throw new IOException(
                    "LZ4 Hadoop decompression size mismatch: expected " + uncompressedSize +
                            ", got " + destOffset);
        }

        return uncompressed;
    }

    /**
     * Decompress using raw LZ4 block format (no framing).
     */
    private byte[] decompressRaw(MappedByteBuffer compressed, int uncompressedSize) {
        byte[] uncompressed = new byte[uncompressedSize];
        ByteBuffer dest = ByteBuffer.wrap(uncompressed);
        fastDecompressor.decompress(compressed, 0, dest, 0, uncompressedSize);
        return uncompressed;
    }

    @Override
    public String getName() {
        return "LZ4";
    }
}
