/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;

import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.internal.metadata.DictionaryPageHeader;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// Parses dictionary pages from column chunk data.
///
/// Extracted from [PageScanner] so that both [IndexedFetchPlan] and
/// [PageScanner] can reuse the same parsing logic.
public final class DictionaryParser {

    private DictionaryParser() {}

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
            throw new ParquetReadException("Invalid dictionary page for column '"
                    + columnSchema.name() + "': page type is " + header.type());
        }

        int compressedSize = header.compressedPageSize();
        if (compressedData.remaining() != compressedSize) {
            throw new ParquetReadException("Invalid dictionary page for column '"
                    + columnSchema.name() + "': body of " + compressedData.remaining()
                    + " bytes, header claims " + compressedSize);
        }

        DictionaryPageHeader dictionaryPageHeader = header.dictionaryPageHeader();
        if (dictionaryPageHeader == null) {
            throw new ParquetReadException("Invalid dictionary page for column '"
                    + columnSchema.name() + "': no dictionary_page_header");
        }

        int numValues = dictionaryPageHeader.numValues();
        if (numValues < 0) {
            throw new ParquetReadException("Invalid dictionary page for column '"
                    + columnSchema.name() + "': negative numValues (" + numValues + ")");
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
        try {
            byte[] data = decompressor.decompress(compressedData, uncompressedSize);
            return Dictionary.parse(data, numValues, column.type(), column.typeLength());
        }
        catch (UnsupportedOperationException e) {
            // A decompressor that exists but will not run here — libdeflate below Java 22 —
            // says the same thing an absent one does, so it leaves as it arrived.
            throw e;
        }
        catch (RuntimeException e) {
            // The column is read context, added once on the way out; naming it
            // here as well said it twice in one sentence.
            throw new ParquetReadException("Failed to parse dictionary"
                    + " (type=" + column.type()
                    + ", numValues=" + numValues
                    + ", uncompressedSize=" + uncompressedSize
                    + ", compressedSize=" + compressedData.remaining()
                    + ", codec=" + codec + ")", e);
        }
    }
}
