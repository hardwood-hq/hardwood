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
import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.internal.metadata.DictionaryPageHeader;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.internal.thrift.ThriftTruncatedException;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// Parses dictionary pages from column chunk data.
///
/// Shared by [IndexedFetchPlan], [SequentialFetchPlan], the dictionary
/// filter's `RowGroupDictionaryFilterSource` and the CLI's `dive`.
public final class DictionaryParser {

    /// Bytes read speculatively when the offsets give no gap to size the dictionary read — an
    /// absent `dictionary_page_offset`. Covers a page header plus the body of all but unusually
    /// large dictionaries, so the common case still resolves in a single read.
    private static final int DICTIONARY_PROBE_BYTES = 64 * 1024;

    /// Bytes first read for a dictionary page header alone. A thrift-compact page header runs to a
    /// few dozen bytes, so this is one short read; a longer header widens it.
    private static final int HEADER_PROBE_BYTES = 256;

    private DictionaryParser() {}

    /// Reads a column chunk's dictionary page, header and compressed body, or returns `null` when
    /// the chunk's first page is not a dictionary page.
    ///
    /// The page's own header states its length; the offsets only size the opening read — the gap
    /// to the first data page where there is one, a bounded probe otherwise. For a well-formed
    /// chunk the gap equals the page's length, so the opening read is exact and no second one
    /// happens. Only the page is read, never the rest of the chunk, so a chunk of any size can be
    /// read this way.
    ///
    /// A header-declared length is file data, so it is checked against the enclosing column chunk
    /// before being used to size a read. A page that claims to run past its own chunk is corrupt
    /// and is rejected here — truncating the read to the chunk instead would fail later and less
    /// clearly.
    ///
    /// @param messagePrefix put in front of the message of each failure this method raises
    ///        itself, naming where the chunk is; may be empty
    /// @throws ParquetReadException if the offsets or the page header contradict the chunk
    public static ByteBuffer readPage(InputFile inputFile, ColumnChunk columnChunk, String messagePrefix)
            throws IOException {
        Extent extent = extent(columnChunk, messagePrefix);
        ByteBuffer region = inputFile.readRange(extent.start(),
                Math.toIntExact(Math.min(extent.openingRead(), extent.availableBytes())));
        int pageLength = pageLength(region);
        if (pageLength < 0) {
            return null;
        }
        if (pageLength > extent.availableBytes()) {
            throw new ParquetReadException(messagePrefix
                    + "Malformed Parquet metadata: the dictionary page header declares "
                    + pageLength + " bytes but only " + extent.availableBytes()
                    + " bytes remain in the chunk");
        }
        return pageLength > region.remaining()
                ? inputFile.readRange(extent.start(), pageLength)
                : region.slice(0, pageLength);
    }

    /// Reads the header of a column chunk's dictionary page, or returns `null` when the chunk's
    /// first page is not a dictionary page.
    ///
    /// The page is located as [#readPage] locates it, and only its header is read: a short probe,
    /// widened while the header does not fit in it, up to the end of the chunk or
    /// [PageFormatProbe#MAX_PEEK_SIZE], whichever comes first.
    ///
    /// @param messagePrefix as [#readPage]
    /// @throws ParquetReadException as [#readPage], or if the header does not fit in the chunk or
    ///         exceeds [PageFormatProbe#MAX_PEEK_SIZE]
    public static PageHeader readPageHeader(InputFile inputFile, ColumnChunk columnChunk, String messagePrefix)
            throws IOException {
        Extent extent = extent(columnChunk, messagePrefix);
        int ceiling = Math.min(PageFormatProbe.MAX_PEEK_SIZE, extent.availableBytes());
        int probe = Math.min(HEADER_PROBE_BYTES, ceiling);
        while (true) {
            try {
                PageHeader header = PageHeaderReader.read(
                        new ThriftCompactReader(inputFile.readRange(extent.start(), probe)));
                return header.type() == PageType.DICTIONARY_PAGE ? header : null;
            }
            catch (ThriftTruncatedException e) {
                if (probe >= extent.availableBytes()) {
                    throw e;
                }
                if (probe >= ceiling) {
                    throw new ParquetReadException(messagePrefix + "Page header at offset " + extent.start()
                            + " exceeds maximum peek size (" + ceiling + " bytes)", e);
                }
                probe = Math.min(2 * probe, ceiling);
            }
        }
    }

    /// Where a chunk's first page starts, how many bytes the chunk leaves it, and how much to read
    /// first to take in the whole of a dictionary page there.
    ///
    /// The available bytes are a bound on the page, whose own size is an int; a chunk over 2 GB
    /// bounds it no tighter than that.
    private static Extent extent(ColumnChunk columnChunk, String messagePrefix) {
        ColumnMetaData metaData = columnChunk.metaData();
        long dataPageOffset = metaData.dataPageOffset();
        long chunkStart;
        try {
            chunkStart = firstPageOffset(columnChunk, dataPageOffset);
        }
        catch (ParquetReadException e) {
            throw new ParquetReadException(messagePrefix + e.getMessage(), e);
        }
        long chunkEnd = chunkStart + metaData.totalCompressedSize();
        if (chunkEnd <= chunkStart) {
            throw new ParquetReadException(messagePrefix
                    + "Malformed Parquet metadata: the dictionary page is at offset " + chunkStart
                    + " but the chunk ends at offset " + chunkEnd);
        }
        int availableBytes = Math.toIntExact(Math.min(chunkEnd - chunkStart, Integer.MAX_VALUE));
        long openingRead = dataPageOffset > chunkStart ? dataPageOffset - chunkStart : DICTIONARY_PROBE_BYTES;
        return new Extent(chunkStart, availableBytes, openingRead);
    }

    private record Extent(long start, int availableBytes, long openingRead) {
    }

    /// Total byte length of the dictionary page starting at the beginning of `region` — its header
    /// plus its compressed body — or `-1` when the region does not begin with a dictionary page.
    ///
    /// A page header states its own body length, so a caller holding only the page's start offset
    /// can read a bounded probe, call this, and learn exactly how many bytes the page occupies
    /// without knowing where the following page begins.
    ///
    /// @param region buffer whose first bytes are a page header
    /// @return the page's total length in bytes, or `-1` if it is not a dictionary page
    public static int pageLength(ByteBuffer region) {
        ThriftCompactReader reader = new ThriftCompactReader(region, 0);
        PageHeader header = PageHeaderReader.read(reader);

        if (header.type() != PageType.DICTIONARY_PAGE) {
            return -1;
        }
        return reader.getBytesRead() + header.compressedPageSize();
    }

    /// Where a column chunk's first page starts, which is where its dictionary page is when it
    /// has one: the declared `dictionary_page_offset`, or `data_page_offset` when the file
    /// declares none.
    ///
    /// The fallback is not a guess. `dictionary_page_offset` is optional in parquet.thrift, so
    /// its absence is ordinary rather than corrupt: parquet-mr 1.12 omits it on every
    /// PLAIN_DICTIONARY column of alltypes_tiny_pages.parquet in apache/parquet-testing, and
    /// Trino did too before 427.
    ///
    /// @param firstDataPageOffset where the chunk's first data page starts
    /// @throws ParquetReadException if the declared dictionary page lies after the first data
    ///         page, where it cannot be read ahead of the pages that need it
    public static long firstPageOffset(ColumnChunk columnChunk, long firstDataPageOffset) {
        Long declared = columnChunk.metaData().dictionaryPageOffset();
        if (declared != null && declared > firstDataPageOffset) {
            throw new ParquetReadException("Malformed Parquet metadata: the dictionary page at offset "
                    + declared + " lies after the first data page at offset " + firstDataPageOffset);
        }
        return columnChunk.chunkStartOffset();
    }

    /// Where a column chunk's dictionary page starts, or `0` when the chunk has none: the page
    /// spans `[start, firstDataPageOffset)`.
    ///
    /// @param firstDataPageOffset where the chunk's first data page starts, as its OffsetIndex
    ///        lists it
    /// @throws ParquetReadException as [#firstPageOffset]
    public static long dictionaryPageStart(ColumnChunk columnChunk, long firstDataPageOffset) {
        long firstPage = firstPageOffset(columnChunk, firstDataPageOffset);
        return firstPage < firstDataPageOffset ? firstPage : 0;
    }

    /// Parses a dictionary page from a byte region covering the dictionary area
    /// of a column chunk (the bytes between the chunk start and the first data page).
    ///
    /// @param dictRegion buffer covering the dictionary region
    /// @param columnSchema the column schema (for type info)
    /// @param metaData the column metadata (for codec)
    /// @param context the hardwood context (for decompressor)
    /// @return the parsed dictionary, or `null` if no dictionary page is found
    public static Dictionary parse(ByteBuffer dictRegion, ColumnSchema columnSchema,
                            ColumnMetaData metaData, HardwoodContextImpl context) {
        ThriftCompactReader probeReader = new ThriftCompactReader(dictRegion, 0);
        PageHeader header = PageHeaderReader.read(probeReader);

        if (header.type() != PageType.DICTIONARY_PAGE) {
            return null;
        }

        int headerSize = probeReader.getBytesRead();
        ByteBuffer compressedData = dictRegion.slice(headerSize, header.compressedPageSize());

        return parsePage(header, compressedData, columnSchema, metaData, context);
    }

    /// Parses a dictionary page whose header the caller has already read.
    ///
    /// A caller scanning a column chunk page by page parses the header anyway, to learn where
    /// the page ends. Handing it back here is what keeps the header from being parsed — and its
    /// CRC computed — a second time.
    ///
    /// This is the one place the page's own claims are checked, so every entry point validates
    /// them alike: [#parse] reaches it too, once it has found a dictionary page to hand over.
    ///
    /// Both the page type and the body's length are checked rather than trusted: a body that is
    /// not the one the header describes — a region handed over whole, say — decodes to values
    /// that are wrong rather than absent, and on the files that carry no page CRC there is
    /// nothing else left to catch it.
    ///
    /// @param header the page header, already parsed, with [PageType#DICTIONARY_PAGE] as its type
    /// @param compressedData the page body alone, `header.compressedPageSize()` bytes,
    ///        with the header excluded
    /// @param columnSchema the column schema (for type info)
    /// @param metaData the column metadata (for codec)
    /// @param context the hardwood context (for decompressor)
    /// @return the parsed dictionary
    /// @throws ParquetReadException if the page is not the one the header describes, if the
    ///         header contradicts itself, or if the body cannot be decoded
    public static Dictionary parsePage(PageHeader header, ByteBuffer compressedData,
                            ColumnSchema columnSchema, ColumnMetaData metaData,
                            HardwoodContextImpl context) {
        if (header.type() != PageType.DICTIONARY_PAGE) {
            throw new ParquetReadException(
                    "Invalid dictionary page: page type is " + header.type());
        }

        int compressedSize = header.compressedPageSize();
        if (compressedData.remaining() != compressedSize) {
            throw new ParquetReadException("Invalid dictionary page: body of "
                    + compressedData.remaining() + " bytes, header claims " + compressedSize);
        }

        DictionaryPageHeader dictionaryPageHeader = header.dictionaryPageHeader();
        if (dictionaryPageHeader == null) {
            throw new ParquetReadException(
                    "Invalid dictionary page: no dictionary_page_header");
        }

        int numValues = dictionaryPageHeader.numValues();
        if (numValues < 0) {
            throw new ParquetReadException(
                    "Invalid dictionary page: negative numValues (" + numValues + ")");
        }

        if (header.crc() != null) {
            CrcValidator.assertCorrectCrc(header.crc(), compressedData);
        }

        return decompress(compressedData, numValues, header.uncompressedPageSize(),
                columnSchema, metaData.codec(), context);
    }

    /// Parses a dictionary from a buffer given the chunk layout. Locates the
    /// dictionary region between `dictAreaStart` and `firstDataPageOffset`,
    /// slices it from the buffer, and parses.
    ///
    /// @param buffer the buffer containing the chunk data
    /// @param bufferFileOffset absolute file offset of the buffer's start
    /// @param dictAreaStart absolute file offset where the dictionary region starts
    /// @param firstDataPageOffset absolute file offset of the first data page
    /// @param columnSchema the column schema
    /// @param metaData the column metadata
    /// @param context the hardwood context
    /// @return the parsed dictionary, or `null` if no dictionary
    static Dictionary parseFromBuffer(ByteBuffer buffer, long bufferFileOffset,
                                       long dictAreaStart, long firstDataPageOffset,
                                       ColumnSchema columnSchema, ColumnMetaData metaData,
                                       HardwoodContextImpl context) {
        int dictRelOffset = Math.toIntExact(dictAreaStart - bufferFileOffset);
        int dictRegionSize = Math.toIntExact(firstDataPageOffset - dictAreaStart);
        ByteBuffer dictRegion = buffer.slice(dictRelOffset, dictRegionSize);
        return parse(dictRegion, columnSchema, metaData, context);
    }

    private static Dictionary decompress(ByteBuffer compressedData, int numValues,
                                          int uncompressedSize, ColumnSchema column,
                                          CompressionCodec codec,
                                          HardwoodContextImpl context) {
        // An absent codec library leaves as it arrived. It names the dependency to
        // add, which is the only actionable thing about it, and it is not the file
        // being wrong — so it must not be caught and re-typed as one.
        Decompressor decompressor = context.decompressorFactory().getDecompressor(codec);
        // Taken ahead of the decompressor, which consumes the buffer.
        int compressedSize = compressedData.remaining();
        try {
            byte[] data = decompressor.decompress(compressedData, uncompressedSize);
            return Dictionary.parse(data, uncompressedSize, numValues, column.type(), column.typeLength());
        }
        catch (UnsupportedOperationException e) {
            // A decompressor that exists but will not run here — libdeflate below Java 22 —
            // says the same thing an absent one does, so it leaves as it arrived.
            throw e;
        }
        catch (RuntimeException e) {
            throw new ParquetReadException("Failed to parse dictionary (type=" + column.type()
                    + ", numValues=" + numValues
                    + ", uncompressedSize=" + uncompressedSize
                    + ", compressedSize=" + compressedSize
                    + ", codec=" + codec + ")", e);
        }
    }
}
