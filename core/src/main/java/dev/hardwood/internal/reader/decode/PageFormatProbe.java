/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Predicate;

import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.step.ByteSource;
import dev.hardwood.internal.reader.step.Parsed;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.internal.thrift.ThriftTruncatedException;
import dev.hardwood.reader.ParquetReadException;

/// Reads a page header from the bytes a read holds, widening its peek while the header does not
/// fit ([#readHeader]): for the scan of a column chunk without an OffsetIndex, for
/// [DictionaryParser#readPageHeader], and for walking a chunk header to header ([#walk]), as the
/// CLI does.
public final class PageFormatProbe {

    /// Initial peek size for a page header. One KiB covers a typical header
    /// without inline `min_value`/`max_value` binaries.
    public static final int INITIAL_PEEK_SIZE = 1024;

    /// Upper bound on the peek size. Headers carrying long inline statistics
    /// rarely exceed a few KiB; 1 MiB is comfortably beyond that and protects
    /// against runaway reads on a corrupt file. Also bounds the CLI's page-header walk.
    public static final int MAX_PEEK_SIZE = 1024 * 1024;

    private PageFormatProbe() {
    }

    /// Walks the pages of `[chunkStart, chunkEnd)` header to header, handing each header to
    /// `visitor` until it returns `false` or the chunk ends; or the range it needs that `bytes`
    /// does not hold, from which a caller walks again once it does. Only headers are read: a
    /// page's body is stepped over.
    ///
    /// @return `true` when the visitor stopped the walk, `false` when the chunk ended
    /// @throws ParquetReadException if a header runs past the end of the chunk or exceeds
    ///         [#MAX_PEEK_SIZE], or a page's body runs past the end of the chunk
    public static Parsed<Boolean> walk(ByteSource bytes, long chunkStart, long chunkEnd,
                                       Predicate<PageHeader> visitor) throws IOException {
        long offset = chunkStart;
        while (offset < chunkEnd) {
            Parsed<HeaderAt> read = readHeader(bytes, offset, chunkEnd - offset, INITIAL_PEEK_SIZE, "");
            if (read.needsBytes()) {
                return read.asNeed();
            }
            HeaderAt parsed = read.value();
            long pageEnd = pageEnd(parsed, offset, chunkEnd);
            if (!visitor.test(parsed.header())) {
                return Parsed.of(true);
            }
            offset = pageEnd;
        }
        return Parsed.of(false);
    }

    /// The offset at which the page whose header `parsed` starts at `offset` ends: past its header and
    /// the body its `compressed_page_size` declares.
    ///
    /// @throws ParquetReadException if the page ends past `chunkEnd`, the end of its column chunk
    public static long pageEnd(HeaderAt parsed, long offset, long chunkEnd) {
        long pageEnd = offset + parsed.length() + parsed.header().compressedPageSize();
        if (pageEnd > chunkEnd) {
            throw new ParquetReadException("Page at offset " + offset + " ends at offset " + pageEnd
                    + ", past the end of the column chunk at offset " + chunkEnd);
        }
        return pageEnd;
    }

    /// The page header at `offset`, starting from a peek of `fromPeek` bytes and doubling it while the
    /// header does not fit, up to [#MAX_PEEK_SIZE] or the bytes remaining in the chunk; or the
    /// range it needs that `bytes` does not hold. The length of that range is the peek reached,
    /// from which a caller resuming the read starts again.
    ///
    /// @param remaining the bytes from `offset` to the end of the column chunk
    /// @param messagePrefix put in front of the message of each failure this method raises,
    ///        naming where the chunk is; may be empty
    /// @throws ParquetReadException if the header runs past the end of the chunk or exceeds
    ///         [#MAX_PEEK_SIZE]
    public static Parsed<HeaderAt> readHeader(ByteSource bytes, long offset, long remaining, int fromPeek,
                                       String messagePrefix) throws IOException {
        int peekCeiling = Math.toIntExact(Math.min(MAX_PEEK_SIZE, remaining));
        int peek = Math.min(fromPeek, peekCeiling);
        while (true) {
            ByteBuffer buf = bytes.read(offset, peek);
            if (buf == null) {
                return Parsed.need(offset, peek);
            }
            ThriftCompactReader reader = new ThriftCompactReader(buf);
            try {
                PageHeader header = PageHeaderReader.read(reader);
                return Parsed.of(new HeaderAt(header, reader.getBytesRead()));
            }
            catch (ThriftTruncatedException truncated) {
                if (peek >= remaining) {
                    throw new ParquetReadException(messagePrefix + "Page header at offset " + offset
                            + " does not fit in the " + remaining
                            + " bytes left in its column chunk — the file is likely corrupt", truncated);
                }
                if (peek >= peekCeiling) {
                    throw new ParquetReadException(messagePrefix + "Page header at offset " + offset
                            + " exceeds maximum peek size (" + MAX_PEEK_SIZE + " bytes)", truncated);
                }
                peek = Math.min(peekCeiling, peek * 2);
            }
        }
    }

    /// A parsed page header and its encoded length in bytes.
    public record HeaderAt(PageHeader header, int length) {
    }
}
