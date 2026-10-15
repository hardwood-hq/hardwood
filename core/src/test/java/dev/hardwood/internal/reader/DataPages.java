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
        return decodeRequired(encoding, dataPageHeader(numValues, body, encoding), type, typeLength,
                numValues, body);
    }

    /// Decodes a page as the overload above does, over a column with no type length, its header
    /// declaring the raw Thrift encoding value `thriftValue`, which [Encoding] need not name.
    ///
    /// The header writer takes an [Encoding] only, so the header is written twice with encodings
    /// whose values differ, and the one byte the two disagree on (the encoding's zigzag varint)
    /// is replaced. A value below 64 is a single byte in that form.
    static Page decodeRequired(int thriftValue, PhysicalType type, int numValues, byte[] body) throws Exception {
        if (thriftValue < 0 || thriftValue >= 64) {
            throw new IllegalArgumentException("Thrift value must be in [0, 64): " + thriftValue);
        }
        byte[] plain = dataPageHeader(numValues, body, Encoding.PLAIN);
        byte[] other = dataPageHeader(numValues, body, Encoding.RLE_DICTIONARY);
        int differing = -1;
        for (int i = 0; i < plain.length; i++) {
            if (plain[i] != other[i]) {
                if (differing != -1) {
                    throw new IllegalStateException("Headers differ in more than one byte");
                }
                differing = i;
            }
        }
        plain[differing] = (byte) (thriftValue << 1);
        return decodeRequired(Encoding.UNKNOWN, plain, type, null, numValues, body);
    }

    private static byte[] dataPageHeader(int numValues, byte[] body, Encoding encoding) {
        CRC32 crc = new CRC32();
        crc.update(body);
        ThriftCompactWriter header = new ThriftCompactWriter();
        PageHeaderWriter.writeDataPageV1(header, numValues, body.length, body.length,
                (int) crc.getValue(), encoding);
        return header.toByteArray();
    }

    private static Page decodeRequired(Encoding encoding, byte[] headerBytes, PhysicalType type,
                                       Integer typeLength, int numValues, byte[] body) throws Exception {
        ColumnSchema column = new ColumnSchema(FieldPath.of("c"), type, RepetitionType.REQUIRED,
                typeLength, 0, 0, 0, null);
        ColumnMetaData metaData = new ColumnMetaData(type, List.of(encoding), FieldPath.of("c"),
                CompressionCodec.UNCOMPRESSED, numValues, 0, 0, Map.of(), 0, null, null, null,
                null, null, List.of(), null);

        ByteBuffer page = ByteBuffer.allocate(headerBytes.length + body.length);
        page.put(headerBytes).put(body).flip();

        return new PageDecoder(metaData, column, new DecompressorFactory(null)).decodePage(page, null);
    }
}
