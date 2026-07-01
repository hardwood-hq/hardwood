/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.metadata.Encoding;

/// Writer for a DataPage V1 PageHeader to Thrift Compact Protocol, the inverse of
/// the PageHeader/DataPageHeader parsing in [PageHeaderReader] and
/// [DataPageHeaderReader].
public class PageHeaderWriter {

    /// Thrift `PageType.DATA_PAGE`.
    private static final int PAGE_TYPE_DATA_PAGE = 0;

    /// Serialize a DataPage V1 page header.
    ///
    /// @param writer the destination
    /// @param numValues number of values in the page
    /// @param uncompressedSize uncompressed size of the page body (levels + values)
    /// @param compressedSize compressed size of the page body; equals the
    ///        uncompressed size when the page is stored uncompressed
    /// @param crc CRC-32 of the page body as stored (the bytes the reader validates)
    /// @param valuesEncoding the encoding of the values
    public static void writeDataPageV1(ThriftCompactWriter writer, int numValues,
                                       int uncompressedSize, int compressedSize, int crc, Encoding valuesEncoding) {
        writer.pushFieldIdContext();

        // 1: type
        writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(PAGE_TYPE_DATA_PAGE);

        // 2: uncompressed_page_size
        writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(uncompressedSize);

        // 3: compressed_page_size
        writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(compressedSize);

        // 4: crc (CRC-32 of the page body as stored on disk)
        writer.writeFieldBegin(4, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(crc);

        // 5: data_page_header
        writer.writeFieldBegin(5, ThriftCompactConstants.FieldType.STRUCT);
        writeDataPageHeader(writer, numValues, valuesEncoding);

        writer.writeFieldStop();
    }

    private static void writeDataPageHeader(ThriftCompactWriter writer, int numValues, Encoding valuesEncoding) {
        short saved = writer.pushFieldIdContext();
        try {
            // 1: num_values
            writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
            writer.writeI32(numValues);

            // 2: encoding
            writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32);
            writer.writeI32(ThriftEnumLookup.thriftValue(valuesEncoding));

            // 3: definition_level_encoding (RLE). For a REQUIRED column there are
            // no definition levels, but the field is required by the V1 header.
            writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.I32);
            writer.writeI32(ThriftEnumLookup.thriftValue(Encoding.RLE));

            // 4: repetition_level_encoding (RLE). Likewise no repetition levels
            // for a flat column.
            writer.writeFieldBegin(4, ThriftCompactConstants.FieldType.I32);
            writer.writeI32(ThriftEnumLookup.thriftValue(Encoding.RLE));

            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }
}
