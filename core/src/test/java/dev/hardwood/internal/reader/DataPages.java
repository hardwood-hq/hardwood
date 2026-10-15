/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.thrift.PageHeaderWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;

/// Decodes hand-built `DATA_PAGE`s through [PageDecoder], for tests of what it makes of a page body.
final class DataPages {

    private DataPages() {
    }

    /// Decodes an uncompressed V1 data page of `numValues` values over a required column `c` of
    /// `type`, its value section `body`.
    ///
    /// @param typeLength the column's value width for `FIXED_LEN_BYTE_ARRAY`, `null` otherwise
    static Page decodeRequired(Encoding encoding, PhysicalType type, Integer typeLength, int numValues,
                               byte[] body) throws Exception {
        ColumnSchema column = new ColumnSchema(FieldPath.of("c"), type, RepetitionType.REQUIRED,
                typeLength, 0, 0, 0, null);
        ColumnMetaData metaData = new ColumnMetaData(type, List.of(encoding), FieldPath.of("c"),
                CompressionCodec.UNCOMPRESSED, numValues, 0, 0, Map.of(), 0, null, null, null,
                null, null, List.of(), null);

        CRC32 crc = new CRC32();
        crc.update(body);
        ThriftCompactWriter header = new ThriftCompactWriter();
        PageHeaderWriter.writeDataPageV1(header, numValues, body.length, body.length,
                (int) crc.getValue(), encoding);
        byte[] headerBytes = header.toByteArray();

        ByteBuffer page = ByteBuffer.allocate(headerBytes.length + body.length);
        page.put(headerBytes).put(body).flip();

        return new PageDecoder(metaData, column, new DecompressorFactory(null)).decodePage(page, null);
    }
}
