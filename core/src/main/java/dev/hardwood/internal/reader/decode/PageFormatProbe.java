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
import dev.hardwood.internal.thrift.PageLimit;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.internal.thrift.ThriftTruncatedException;
import dev.hardwood.reader.ParquetReadException;

/// Reads a page header within its extent, widening its peek while the header does not fit
/// ([#readHeader]): every reader of page headers reads them through here, the read's planned and
/// scanned pages, a chunk's dictionary page, and the CLI's walk of a chunk header to header
/// ([#walk]). A header is valid when it parses within its extent; no other bound applies.
public final class PageFormatProbe {

    /// Initial peek size for a page header. One KiB covers a typical header
    /// without inline `min_value`/`max_value` binaries.
    public static final int INITIAL_PEEK_SIZE = 1024;

    /// What a header and its page's body lie in: the page, where its size is known, or the rest of
    /// its column chunk.
    public enum Extent {
        PAGE("page"),
        CHUNK("column chunk");

        private final String noun;

        Extent(String noun) {
            this.noun = noun;
        }
    }

    private PageFormatProbe() {
    }

    /// Walks the pages of `[chunkStart, chunkEnd)` header to header, handing each header to
    /// `visitor` until it returns `false` or the chunk ends; or the range it needs that `bytes`
    /// does not hold, from which a caller walks again once it does. Only headers are read: a
    /// page's body is stepped over.
    ///
    /// @return `true` when the visitor stopped the walk, `false` when the chunk ended
    /// @throws ParquetReadException if a header or a page's body runs past the end of the chunk
    public static Parsed<Boolean> walk(ByteSource bytes, long chunkStart, long chunkEnd,
                                       Predicate<PageHeader> visitor) throws IOException {
        long offset = chunkStart;
        while (offset < chunkEnd) {
            Parsed<HeaderAt> read = readHeader(bytes, offset, chunkEnd - offset, Extent.CHUNK, INITIAL_PEEK_SIZE, "");
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

    /// The page header at `offset`, starting from a peek of `fromPeek` bytes and widening it while the
    /// header does not fit, up to the end of its extent or the largest page there is
    /// ([PageLimit#MAX_PAGE_BYTES]), whichever is nearer; or the range it needs that `bytes` does not
    /// hold. The length of that range is the peek reached, from which a caller resuming the read
    /// starts again. A peek widens past the bytes a truncated read declares it needs, a binary
    /// value's length, and doubles otherwise, so that a declared length past the ceiling fails
    /// without reading it.
    ///
    /// @param remaining the bytes from `offset` to the end of `extent`
    /// @param messagePrefix put in front of the message of each failure this method raises,
    ///        naming where the chunk is; may be empty
    /// @throws ParquetReadException if the header does not parse within its extent
    public static Parsed<HeaderAt> readHeader(ByteSource bytes, long offset, long remaining, Extent extent,
                                              int fromPeek, String messagePrefix) throws IOException {
        if (fromPeek <= 0) {
            throw new IllegalArgumentException("A peek must be positive but was " + fromPeek);
        }
        int peekCeiling = Math.toIntExact(Math.min(PageLimit.MAX_PAGE_BYTES, remaining));
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
                if (peek >= peekCeiling || truncated.needed() > peekCeiling) {
                    String room = peekCeiling < remaining ? peekCeiling + " bytes, the most a page holds"
                            : "the " + remaining + " bytes left in its " + extent.noun;
                    throw new ParquetReadException(messagePrefix + "Page header at offset " + offset
                            + " does not fit in " + room + " — the file is likely corrupt", truncated);
                }
                // past a declared length, a first peek's worth for the fields after it
                long needed = truncated.needed() < 0 ? -1 : truncated.needed() + INITIAL_PEEK_SIZE;
                peek = Math.toIntExact(Math.min(peekCeiling, Math.max(2L * peek, needed)));
            }
        }
    }

    /// A parsed page header and its encoded length in bytes.
    public record HeaderAt(PageHeader header, int length) {
    }
}
