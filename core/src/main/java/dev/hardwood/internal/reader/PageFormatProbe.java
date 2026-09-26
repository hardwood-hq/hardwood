/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;

import dev.hardwood.InputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.internal.thrift.ThriftTruncatedException;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetReadException;

/// Reads just enough of a column chunk to identify its first data page's
/// format (v1 vs v2). Used by the per-page mask gate in [RowGroupIterator] to
/// distinguish nested-v1 columns — whose repetition levels live inside the
/// compressed area and would require decompression to count records — from
/// nested-v2 columns, whose repetition levels live in an uncompressed prefix
/// and can be walked without invoking the codec.
///
/// All data pages in a single column chunk share the same format in practice
/// (Parquet writers don't mix v1 and v2 within one chunk), so a single peek
/// at the first data page is authoritative.
public final class PageFormatProbe {

    /// Initial peek size for a page header. One KiB covers a typical header
    /// without inline `min_value`/`max_value` binaries.
    ///
    /// Shared with [SequentialFetchPlan]'s page-header reader: both peek
    /// the same kind of bytes from the same kind of place, so the budget
    /// lives in one place instead of being duplicated.
    static final int INITIAL_PEEK_SIZE = 1024;

    /// Upper bound on the peek size. Headers carrying long inline statistics
    /// rarely exceed a few KiB; 1 MiB is comfortably beyond that and protects
    /// against runaway reads on a corrupt file. Also bounds the header reads made outside the
    /// pipeline, by [DictionaryParser#readPageHeader] and the CLI's page-header walk.
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
    /// [SequentialFetchPlan] walks the chunk.
    static PageType firstDataPageType(InputFile inputFile,
                                      ColumnChunk columnChunk) throws IOException {
        long chunkStart = columnChunk.chunkStartOffset();
        long chunkEnd = chunkStart + columnChunk.metaData().totalCompressedSize();
        PageType declared = dataPageAt(inputFile, columnChunk.metaData().dataPageOffset(), chunkStart, chunkEnd);
        return declared != null ? declared : walkToFirstDataPage(inputFile, chunkStart, chunkEnd);
    }

    /// The type of the data page whose header is at `offset`, read in a single peek, or `null`
    /// when the bytes there are not a data page header whose page lies within the chunk.
    private static PageType dataPageAt(InputFile inputFile, long offset, long chunkStart, long chunkEnd)
            throws IOException {
        if (offset < chunkStart || offset >= chunkEnd) {
            return null;
        }
        ThriftCompactReader reader = new ThriftCompactReader(
                inputFile.readRange(offset, Math.toIntExact(Math.min(INITIAL_PEEK_SIZE, chunkEnd - offset))));
        PageHeader header;
        try {
            header = PageHeaderReader.read(reader);
        }
        catch (ParquetReadException notAHeader) {
            return null;
        }
        boolean dataPage = (header.type() == PageType.DATA_PAGE && header.dataPageHeader() != null)
                || (header.type() == PageType.DATA_PAGE_V2 && header.dataPageHeaderV2() != null);
        long pageEnd = offset + reader.getBytesRead() + header.compressedPageSize();
        return dataPage && pageEnd <= chunkEnd ? header.type() : null;
    }

    /// Walks the chunk's pages from `chunkStart` to the first data page.
    private static PageType walkToFirstDataPage(InputFile inputFile, long chunkStart, long chunkEnd)
            throws IOException {
        long offset = chunkStart;
        while (offset < chunkEnd) {
            HeaderAt parsed = readHeader(inputFile, offset, chunkEnd - offset);
            PageType type = parsed.header().type();
            if (type != PageType.DICTIONARY_PAGE && type != PageType.INDEX_PAGE) {
                return type;
            }
            long pageEnd = offset + parsed.length() + parsed.header().compressedPageSize();
            if (pageEnd > chunkEnd) {
                throw new ParquetReadException("Page at offset " + offset + " ends at offset " + pageEnd
                        + ", past the end of the column chunk at offset " + chunkEnd);
            }
            offset = pageEnd;
        }
        throw new ParquetReadException("Column chunk at offset " + chunkStart + " has no data page");
    }

    /// Reads the page header at `offset`, growing the peek on truncation up to [#MAX_PEEK_SIZE]
    /// or the bytes remaining in the chunk.
    private static HeaderAt readHeader(InputFile inputFile, long offset, long remaining)
            throws IOException {
        int peekCeiling = Math.toIntExact(Math.min(MAX_PEEK_SIZE, remaining));
        int peek = Math.min(INITIAL_PEEK_SIZE, peekCeiling);
        while (true) {
            ByteBuffer buf = inputFile.readRange(offset, peek);
            ThriftCompactReader reader = new ThriftCompactReader(buf);
            try {
                PageHeader header = PageHeaderReader.read(reader);
                return new HeaderAt(header, reader.getBytesRead());
            }
            catch (ThriftTruncatedException truncated) {
                if (peek >= peekCeiling) {
                    throw new ParquetReadException("Page header at offset " + offset + " exceeds "
                            + peekCeiling + " bytes — the file is likely corrupt", truncated);
                }
                peek = Math.min(peekCeiling, peek * 2);
            }
        }
    }

    /// A parsed page header and its encoded length in bytes.
    private record HeaderAt(PageHeader header, int length) {
    }
}
