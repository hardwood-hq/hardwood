/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// A page's bytes, header and body as stored, with its header parsed, so that whoever parsed it to
/// locate the page hands it on as parsed: a data page, whose fetch plan parses the header to
/// locate the page and admit its decode, which takes it as parsed, or a dictionary page, which the
/// dictionary read hands to its parse likewise.
///
/// @param header the page's header
/// @param headerSize the bytes the header takes at the start of `bytes`
/// @param bytes the page, header first
public record DataPage(PageHeader header, int headerSize, ByteBuffer bytes) {

    /// @throws ParquetReadException if the body the header declares runs past `bytes`, which
    ///         whoever located the page sized, the header scan or the chunk's offset index
    public DataPage {
        long room = (long) bytes.remaining() - headerSize;
        if (header.compressedPageSize() > room) {
            throw new ParquetReadException("Page header declares a body of " + header.compressedPageSize()
                    + " bytes, but its page holds " + room + " bytes after its " + headerSize + "-byte header");
        }
    }

    /// The page whose bytes, header first, are `bytes`, the page at `offset` of its file, which is
    /// the header's extent ([PageFormatProbe#readHeader]).
    ///
    /// @throws ParquetReadException if the header does not parse within the page
    public static DataPage parse(ByteBuffer bytes, long offset) {
        try {
            PageFormatProbe.HeaderAt header = PageFormatProbe.readHeader(
                    (o, length) -> bytes.slice(Math.toIntExact(o - offset), length), offset, bytes.remaining(),
                    PageFormatProbe.Extent.PAGE, PageFormatProbe.INITIAL_PEEK_SIZE, "").get();
            return new DataPage(header.header(), header.length(), bytes);
        }
        catch (IOException e) {
            // slicing a buffer reads nothing
            throw new UncheckedIOException(e);
        }
    }

    /// The buffers the task decoding the page `header` heads holds while it runs, from the header
    /// alone, as the working set counts a task's: a dictionary page's decompressed bytes; a data
    /// page's decompressed bytes, levels and scratch ([WorkingSet.Column#pageBuffers]); `0` for a
    /// page of another type.
    public static long taskBytes(PageHeader header, ColumnSchema schema) {
        return switch (header.type()) {
            case DICTIONARY_PAGE -> header.uncompressedPageSize();
            case DATA_PAGE -> WorkingSet.Column.pageBuffers(schema, header.dataPageHeader().numValues(),
                    header.uncompressedPageSize(), header.dataPageHeader().encoding());
            case DATA_PAGE_V2 -> WorkingSet.Column.pageBuffers(schema, header.dataPageHeaderV2().numValues(),
                    header.uncompressedPageSize(), header.dataPageHeaderV2().encoding());
            default -> 0;
        };
    }

    /// The values a data page's header declares.
    ///
    /// @throws IllegalStateException for a page that is not a data page
    public int numValues() {
        return switch (header.type()) {
            case DATA_PAGE -> header.dataPageHeader().numValues();
            case DATA_PAGE_V2 -> header.dataPageHeaderV2().numValues();
            default -> throw notDataPage();
        };
    }

    /// The encoding of a data page's values.
    ///
    /// @throws IllegalStateException for a page that is not a data page
    public Encoding encoding() {
        return switch (header.type()) {
            case DATA_PAGE -> header.dataPageHeader().encoding();
            case DATA_PAGE_V2 -> header.dataPageHeaderV2().encoding();
            default -> throw notDataPage();
        };
    }

    private IllegalStateException notDataPage() {
        return new IllegalStateException("A " + header.type() + " page is not a data page");
    }

    /// The page's body as stored, after its header.
    public ByteBuffer body() {
        return bytes.slice(headerSize, header.compressedPageSize());
    }
}
