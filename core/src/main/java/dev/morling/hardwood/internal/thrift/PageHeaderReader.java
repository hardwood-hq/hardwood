/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.thrift;

import java.io.IOException;

import dev.morling.hardwood.metadata.DataPageHeader;
import dev.morling.hardwood.metadata.DataPageHeaderV2;
import dev.morling.hardwood.metadata.DictionaryPageHeader;
import dev.morling.hardwood.metadata.PageHeader;

/**
 * Reader for PageHeader from Thrift Compact Protocol.
 */
public class PageHeaderReader {

    public static PageHeader read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static PageHeader readInternal(ThriftCompactReader reader) throws IOException {
        PageHeader.PageType type = null;
        int uncompressedPageSize = 0;
        int compressedPageSize = 0;
        DataPageHeader dataPageHeader = null;
        DataPageHeaderV2 dataPageHeaderV2 = null;
        DictionaryPageHeader dictionaryPageHeader = null;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // type
                    if (header.type() == 0x05) {
                        type = PageHeader.PageType.fromThriftValue(reader.readI32());
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // uncompressed_page_size
                    if (header.type() == 0x05) {
                        uncompressedPageSize = reader.readI32();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 3: // compressed_page_size
                    if (header.type() == 0x05) {
                        compressedPageSize = reader.readI32();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 4: // crc (optional) - skipped for now
                    reader.skipField(header.type());
                    break;
                case 5: // data_page_header
                    if (header.type() == 0x0C) {
                        dataPageHeader = DataPageHeaderReader.read(reader);
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 6: // index_page_header (optional) - skipped for now
                    reader.skipField(header.type());
                    break;
                case 7: // dictionary_page_header
                    if (header.type() == 0x0C) {
                        dictionaryPageHeader = DictionaryPageHeaderReader.read(reader);
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 8: // data_page_header_v2
                    if (header.type() == 0x0C) {
                        dataPageHeaderV2 = DataPageHeaderV2Reader.read(reader);
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                default:
                    reader.skipField(header.type());
                    break;
            }
        }

        // Validate required fields
        if (type == null) {
            throw new IOException("PageHeader missing required field: type");
        }

        return new PageHeader(type, uncompressedPageSize, compressedPageSize,
                dataPageHeader, dataPageHeaderV2, dictionaryPageHeader);
    }
}
