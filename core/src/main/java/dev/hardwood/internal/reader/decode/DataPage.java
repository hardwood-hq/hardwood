/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.nio.ByteBuffer;

import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
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

    /// The page whose bytes, header first, are `bytes`.
    ///
    /// @throws dev.hardwood.reader.ParquetReadException if the header does not parse
    public static DataPage parse(ByteBuffer bytes) {
        ThriftCompactReader reader = new ThriftCompactReader(bytes, 0);
        PageHeader header = PageHeaderReader.read(reader);
        return new DataPage(header, reader.getBytesRead(), bytes);
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

    /// The page's body as stored, after its header.
    public ByteBuffer body() {
        return bytes.slice(headerSize, header.compressedPageSize());
    }
}
