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
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetReadException;

/// Reads just enough of a column chunk to identify its first data page's format (v1 vs v2). Used by
/// the per-page mask gate in [dev.hardwood.internal.reader.plan.RowGroupIterator] to distinguish
/// nested-v1 columns — whose repetition levels live inside the compressed area and would require
/// decompression to count records — from nested-v2 columns, whose repetition levels live in an
/// uncompressed prefix and can be walked without invoking the codec.
///
/// All data pages in a single column chunk share the same format in practice
/// (Parquet writers don't mix v1 and v2 within one chunk), so a single peek
/// at the first data page is authoritative.
///
/// Also reads the page headers of [dev.hardwood.internal.reader.fetch.SequentialFetchPlan] and
/// [DictionaryParser#readPageHeader] ([#readHeader]).
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

    /// Returns the page type of the first data page in `columnChunk`.
    ///
    /// Reads one [#INITIAL_PEEK_SIZE] range at the chunk's `data_page_offset` and takes its
    /// answer when the bytes there parse as a data page header whose page ends within the
    /// chunk. Writers have misstated that offset: DuckDB before duckdb/duckdb#10829 understated
    /// it, pointing into the dictionary page, and a chunk without `dictionary_page_offset` names
    /// its dictionary page there. When the header does not parse, is a dictionary or index page,
    /// or overruns the chunk, the probe walks the chunk from its start instead, stepping over
    /// each dictionary or index page by the length its own header states, the way
    /// [dev.hardwood.internal.reader.fetch.SequentialFetchPlan] walks the chunk.
    ///
    /// Parses the bytes `bytes` holds, and returns the range it needs where it does not hold one:
    /// a peek, or a wider one for a header that does not fit.
    public static Parsed<PageType> firstDataPageType(ByteSource bytes, ColumnChunk columnChunk) throws IOException {
        long chunkStart = columnChunk.chunkStartOffset();
        long chunkEnd = chunkStart + columnChunk.metaData().totalCompressedSize();
        Parsed<PageType> declared = dataPageAt(bytes, columnChunk.metaData().dataPageOffset(), chunkStart, chunkEnd);
        return declared.needsBytes() || declared.value() != null
                ? declared
                : walkToFirstDataPage(bytes, chunkStart, chunkEnd);
    }

    /// The type of the data page whose header is at `offset`, read in a single peek, or `null`
    /// when the bytes there are not a data page header whose page lies within the chunk.
    private static Parsed<PageType> dataPageAt(ByteSource bytes, long offset, long chunkStart, long chunkEnd)
            throws IOException {
        if (offset < chunkStart || offset >= chunkEnd) {
            return Parsed.of(null);
        }
        int peek = Math.toIntExact(Math.min(INITIAL_PEEK_SIZE, chunkEnd - offset));
        ByteBuffer peeked = bytes.read(offset, peek);
        if (peeked == null) {
            return Parsed.need(offset, peek);
        }
        ThriftCompactReader reader = new ThriftCompactReader(peeked);
        PageHeader header;
        try {
            header = PageHeaderReader.read(reader);
        }
        catch (ParquetReadException notAHeader) {
            return Parsed.of(null);
        }
        boolean dataPage = (header.type() == PageType.DATA_PAGE && header.dataPageHeader() != null)
                || (header.type() == PageType.DATA_PAGE_V2 && header.dataPageHeaderV2() != null);
        long pageEnd = offset + reader.getBytesRead() + header.compressedPageSize();
        return Parsed.of(dataPage && pageEnd <= chunkEnd ? header.type() : null);
    }

    /// Walks the chunk's pages from `chunkStart` to the first data page.
    private static Parsed<PageType> walkToFirstDataPage(ByteSource bytes, long chunkStart, long chunkEnd)
            throws IOException {
        PageType[] first = new PageType[1];
        Parsed<Boolean> walked = walk(bytes, chunkStart, chunkEnd, header -> {
            PageType type = header.type();
            if (type == PageType.DICTIONARY_PAGE || type == PageType.INDEX_PAGE) {
                return true;
            }
            first[0] = type;
            return false;
        });
        if (walked.needsBytes()) {
            return walked.asNeed();
        }
        if (!walked.value()) {
            throw new ParquetReadException("Column chunk at offset " + chunkStart + " has no data page");
        }
        return Parsed.of(first[0]);
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
            long pageEnd = offset + parsed.length() + parsed.header().compressedPageSize();
            if (pageEnd > chunkEnd) {
                throw new ParquetReadException("Page at offset " + offset + " ends at offset " + pageEnd
                        + ", past the end of the column chunk at offset " + chunkEnd);
            }
            if (!visitor.test(parsed.header())) {
                return Parsed.of(true);
            }
            offset = pageEnd;
        }
        return Parsed.of(false);
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
