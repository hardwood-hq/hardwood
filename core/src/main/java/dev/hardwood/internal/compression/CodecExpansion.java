/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.reader.ParquetReadException;

/// The most bytes a codec's format can expand a page's compressed bytes to, which bounds the
/// uncompressed size a page header may declare before the decompression output is allocated from
/// it.
///
/// Each bound is the format's worst case per input byte, whatever the content:
///
/// - `SNAPPY`: a copy with a two-byte offset is three bytes and copies at most 64, and no element
///   writes more per byte; the length preamble and the literals write less.
/// - `LZ4_RAW`, `LZ4`: each extension byte of a sequence's match length adds at most 255 bytes,
///   and the sequence's token and offset, three bytes, write at most 19 more; literals write a
///   byte per byte. The Hadoop framing of `LZ4` adds block headers and stored blocks, which write
///   less.
/// - `GZIP`: a deflate symbol takes at least one bit and a length-distance pair at least two, for
///   at most 258 bytes, so 1032 per byte; the gzip header and trailer write nothing.
/// - `ZSTD`: a block is a three-byte header and at least one byte of content and writes at most
///   `Block_Maximum_Size`, 128 KiB (RFC 8878), so 32768 bytes per byte; frame headers, checksums
///   and skippable frames write nothing. The bound holds for RFC 8878 frames: a frame of zstd's
///   legacy formats, or one whose blocks write more than the format allows, can exceed it and fails.
/// - `UNCOMPRESSED`: the stored bytes are the uncompressed ones, so the sizes are equal.
///
/// `BROTLI` expands a few bytes further than a bound would usefully limit, and `LZO` is not read,
/// so a page of theirs is bounded by the read's working-set limit alone.
public final class CodecExpansion {

    /// Bytes a `SNAPPY` copy with a two-byte offset writes at most, from [#SNAPPY_COPY_BYTES].
    static final long SNAPPY_COPY_LENGTH = 64;

    /// Bytes of a `SNAPPY` copy with a two-byte offset: its tag and its offset.
    static final long SNAPPY_COPY_BYTES = 3;

    /// Bytes an `LZ4` block writes at most per byte.
    static final long LZ4_BYTES_PER_BYTE = 255;

    /// Bytes a deflate stream writes at most per byte: a 258-byte match per two bits.
    static final long DEFLATE_BYTES_PER_BYTE = 1032;

    /// Bytes a `ZSTD` block writes at most per byte: `Block_Maximum_Size` per four bytes.
    static final long ZSTD_BYTES_PER_BYTE = 32768;

    private CodecExpansion() {
    }

    /// The most bytes `compressedBytes` of `codec` decompress to, or [Long#MAX_VALUE] for a codec
    /// without a bound.
    public static long maxUncompressedBytes(CompressionCodec codec, long compressedBytes) {
        return switch (codec) {
            case UNCOMPRESSED -> compressedBytes;
            case SNAPPY -> compressedBytes * SNAPPY_COPY_LENGTH / SNAPPY_COPY_BYTES;
            case LZ4, LZ4_RAW -> compressedBytes * LZ4_BYTES_PER_BYTE;
            case GZIP -> compressedBytes * DEFLATE_BYTES_PER_BYTE;
            case ZSTD -> compressedBytes * ZSTD_BYTES_PER_BYTE;
            case BROTLI, LZO -> Long.MAX_VALUE;
        };
    }

    /// Checks a page's declared uncompressed size against what its stored bytes can decompress
    /// to, before the decompression output is allocated from it. Bytes stored uncompressed must
    /// be exactly the declared size ([UncompressedDecompressor#requireStoredSize]).
    ///
    /// @param what names the declared size in the error, such as `Data page's uncompressed_page_size`
    /// @throws ParquetReadException if `codec` cannot expand `storedBytes` to `uncompressedBytes`
    public static void requireWithinExpansion(String what, CompressionCodec codec, int storedBytes,
                                              int uncompressedBytes) {
        if (codec == CompressionCodec.UNCOMPRESSED) {
            UncompressedDecompressor.requireStoredSize(storedBytes, uncompressedBytes);
            return;
        }
        long bound = maxUncompressedBytes(codec, storedBytes);
        if (uncompressedBytes > bound) {
            throw new ParquetReadException(what + " of " + uncompressedBytes + " bytes exceeds the " + bound
                    + " bytes that " + storedBytes + " bytes of " + codec + " decompress to at most");
        }
    }
}
