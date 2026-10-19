/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import dev.hardwood.reader.ParquetReadException;

/// Decompressor for GZIP-compressed data using Inflater directly for maximum performance.
///
/// This implementation bypasses GZIPInputStream to avoid intermediate buffering and copies,
/// decompressing directly into a pre-allocated output buffer.
public class GzipDecompressor implements Decompressor {

    private static final int GZIP_MAGIC = 0x8b1f;
    private static final int FHCRC = 2;
    private static final int FEXTRA = 4;
    private static final int FNAME = 8;
    private static final int FCOMMENT = 16;

    private static final ThreadLocal<byte[]> OUTPUT_BUFFER = new ThreadLocal<>();

    @Override
    public byte[] decompress(ByteBuffer compressed, int uncompressedSize) {
        return decompress(compressed, uncompressedSize, OutputBuffers.borrow(OUTPUT_BUFFER, uncompressedSize));
    }

    @Override
    public byte[] decompress(ByteBuffer page, int uncompressedSize, byte[] dest) {
        // Members are consumed from a duplicate, so the caller's buffer is left as it was
        ByteBuffer compressed = page.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        int totalDecompressed = 0;

        // Concatenated members are read until the declared size is produced; bytes after it are
        // ignored, as libdeflate and parquet-java ignore them
        while (totalDecompressed < uncompressedSize && compressed.hasRemaining()) {
            int headerEnd = skipGzipHeader(compressed);

            Inflater inflater = new Inflater(true); // true = nowrap (raw deflate)
            try {
                // slice() with indices is absolute - must add current position
                ByteBuffer dataSlice = compressed.slice(compressed.position() + headerEnd, compressed.remaining() - headerEnd);
                inflater.setInput(dataSlice);
                int memberStart = totalDecompressed;
                totalDecompressed = inflateMember(inflater, dest, totalDecompressed, uncompressedSize);
                int trailer = compressed.position() + headerEnd + dataSlice.capacity() - inflater.getRemaining();
                checkTrailer(compressed, trailer, dest, memberStart, totalDecompressed - memberStart);
                // Move past the 8-byte trailer to the next member
                compressed.position(trailer + 8);
            }
            catch (DataFormatException e) {
                throw new ParquetReadException("GZIP decompression failed", e);
            }
            finally {
                inflater.end();
            }
        }

        if (totalDecompressed != uncompressedSize) {
            throw new ParquetReadException("Decompressed size mismatch: expected " + uncompressedSize +
                    ", got " + totalDecompressed);
        }

        return dest;
    }

    /// Inflates one member into `dest` from `offset`, and returns the offset its output ends at. The
    /// member must end within `uncompressedSize`: once `dest` holds that many bytes, one more byte
    /// of output means the page decompresses to more than it declares.
    private static int inflateMember(Inflater inflater, byte[] dest, int offset, int uncompressedSize)
            throws DataFormatException {
        int total = offset;
        while (!inflater.finished()) {
            int decompressed = total < uncompressedSize
                    ? inflater.inflate(dest, total, uncompressedSize - total)
                    : inflater.inflate(new byte[1]);
            if (decompressed == 0) {
                if (inflater.finished()) {
                    break;
                }
                if (inflater.needsInput()) {
                    throw new ParquetReadException("Truncated GZIP data");
                }
                if (inflater.needsDictionary()) {
                    throw new ParquetReadException("GZIP stream requires dictionary");
                }
            }
            else if (total == uncompressedSize) {
                throw new ParquetReadException("GZIP data decompresses to more than the expected " +
                        uncompressedSize + " bytes");
            }
            total += decompressed;
        }
        return total;
    }

    /// Checks a member's trailer: the CRC32 of its output, then its length modulo 2^32, both
    /// little-endian.
    private static void checkTrailer(ByteBuffer compressed, int trailer, byte[] dest, int offset, int length) {
        if (trailer + 8 > compressed.limit()) {
            throw new ParquetReadException("Truncated GZIP trailer");
        }
        CRC32 crc = new CRC32();
        crc.update(dest, offset, length);
        if ((int) crc.getValue() != compressed.getInt(trailer)) {
            throw new ParquetReadException("GZIP member's CRC32 does not match its data");
        }
        int isize = compressed.getInt(trailer + 4);
        if (isize != length) {
            throw new ParquetReadException("GZIP member's trailer gives its length as "
                    + Integer.toUnsignedLong(isize) + " bytes, but it inflated to " + length);
        }
    }

    private int skipGzipHeader(ByteBuffer buffer) {
        int start = buffer.position();
        if (buffer.remaining() < 10) {
            throw new ParquetReadException("GZIP data too short for header");
        }

        // Check magic number
        int magic = (buffer.get(start) & 0xff) | ((buffer.get(start + 1) & 0xff) << 8);
        if (magic != GZIP_MAGIC) {
            throw new ParquetReadException("Not in GZIP format");
        }

        // Check compression method (must be 8 = deflate)
        if (buffer.get(start + 2) != 8) {
            throw new ParquetReadException("Unsupported compression method: " + buffer.get(start + 2));
        }

        int flags = buffer.get(start + 3) & 0xff;
        int offset = 10; // Skip fixed header

        // Skip extra field if present
        if ((flags & FEXTRA) != 0) {
            if (offset + 2 > buffer.remaining()) {
                throw new ParquetReadException("Truncated GZIP extra field");
            }
            int extraLen = (buffer.get(start + offset) & 0xff) | ((buffer.get(start + offset + 1) & 0xff) << 8);
            offset += 2 + extraLen;
        }

        // Skip file name if present
        if ((flags & FNAME) != 0) {
            while (offset < buffer.remaining() && buffer.get(start + offset) != 0) {
                offset++;
            }
            offset++; // Skip null terminator
        }

        // Skip comment if present
        if ((flags & FCOMMENT) != 0) {
            while (offset < buffer.remaining() && buffer.get(start + offset) != 0) {
                offset++;
            }
            offset++; // Skip null terminator
        }

        // Skip header CRC if present
        if ((flags & FHCRC) != 0) {
            offset += 2;
        }

        if (offset >= buffer.remaining()) {
            throw new ParquetReadException("GZIP header extends beyond data");
        }

        return offset;
    }

    @Override
    public String getName() {
        return "GZIP";
    }
}
