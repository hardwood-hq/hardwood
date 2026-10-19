/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression.lz4;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.reader.ParquetReadException;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

/// Decompressor for LZ4 compressed data.
///
/// The Parquet LZ4 codec has historically been ambiguous. A page is read as one raw LZ4 block
/// first and, failing that, in the Hadoop format, which frames each block with its sizes:
///
/// Format: [4-byte uncompressed_len, BE][4-byte compressed_len, BE][compressed data]...
public class Lz4Decompressor implements Decompressor {

    private final LZ4FastDecompressor fastDecompressor;
    private final LZ4SafeDecompressor safeDecompressor;

    public Lz4Decompressor() {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        this.fastDecompressor = factory.fastDecompressor();
        this.safeDecompressor = factory.safeDecompressor();
    }

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize, byte[] dest) {
        // Try raw LZ4 first (most common case), then fall back to Hadoop format
        try {
            return decompressRaw(compressed, uncompressedSize, dest);
        }
        catch (Exception rawFailure) {
            // Fall back to Hadoop's block format - needs byte[] for block parsing
            try {
                byte[] compressedBytes = new byte[compressed.remaining()];
                compressed.duplicate().get(compressedBytes);
                return decompressHadoopFormat(compressedBytes, uncompressedSize, dest);
            }
            catch (Exception hadoopFailure) {
                ParquetReadException failure = new ParquetReadException(
                        "LZ4 decompression failed (tried both raw and Hadoop formats): raw: "
                                + rawFailure.getMessage() + "; Hadoop: " + hadoopFailure.getMessage(),
                        rawFailure);
                failure.addSuppressed(hadoopFailure);
                throw failure;
            }
        }
    }

    /// Decompress using Hadoop's native LZ4 block format.
    ///
    /// Format: [4-byte uncompressed_len, BE][4-byte compressed_len, BE][compressed data]...
    ///
    /// Each block has both the uncompressed and compressed sizes in big-endian format,
    /// followed by the compressed data. The blocks must fill `uncompressedSize` bytes and end with
    /// the page.
    private byte[] decompressHadoopFormat(byte[] compressed, int uncompressedSize, byte[] uncompressed) {
        int srcOffset = 0;
        int destOffset = 0;

        ByteBuffer buffer = ByteBuffer.wrap(compressed).order(ByteOrder.BIG_ENDIAN);

        while (srcOffset < compressed.length) {
            if (srcOffset + 8 > compressed.length) {
                throw new ParquetReadException("Truncated LZ4 Hadoop block header");
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
                throw new ParquetReadException("Invalid LZ4 Hadoop block sizes: uncompressed=" +
                        blockUncompressedSize + ", compressed=" + blockCompressedSize);
            }

            if (blockUncompressedSize > uncompressedSize - destOffset) {
                throw new ParquetReadException("LZ4 Hadoop block ends at byte "
                        + ((long) destOffset + blockUncompressedSize) + ", past the expected "
                        + uncompressedSize + " bytes");
            }

            if (blockCompressedSize > compressed.length - srcOffset) {
                throw new ParquetReadException("LZ4 compressed block extends beyond buffer");
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
                    throw new ParquetReadException("LZ4 block size mismatch: expected " +
                            blockUncompressedSize + ", got " + decompressedLen);
                }
                destOffset += decompressedLen;
            }

            srcOffset += blockCompressedSize;
        }

        if (destOffset != uncompressedSize) {
            throw new ParquetReadException(
                    "LZ4 Hadoop decompression size mismatch: expected " + uncompressedSize +
                            ", got " + destOffset);
        }

        return uncompressed;
    }

    /// Decompress using raw LZ4 block format (no framing).
    private byte[] decompressRaw(ByteBuffer compressed, int uncompressedSize, byte[] dest) {
        int consumed = fastDecompressor.decompress(compressed, compressed.position(), ByteBuffer.wrap(dest), 0,
                uncompressedSize);
        // The decompressor bounds its reads by the buffer's capacity, not its limit, so a block
        // ending anywhere but at the limit is not a raw block of this page
        if (consumed != compressed.remaining()) {
            throw new ParquetReadException("LZ4 raw block ends at byte " + consumed + " of a "
                    + compressed.remaining() + "-byte page");
        }
        return dest;
    }

    @Override
    public String getName() {
        return "LZ4";
    }
}
