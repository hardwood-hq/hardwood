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
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;

/// A data page's bytes, header and body as stored, with its header parsed. The fetch plan parses
/// the header to locate the page and to admit its decode, and the decode takes it as parsed.
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

    /// The page's body as stored, after its header.
    public ByteBuffer body() {
        return bytes.slice(headerSize, header.compressedPageSize());
    }
}
