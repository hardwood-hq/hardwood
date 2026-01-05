/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.parquet.metadata;

import java.io.IOException;

import dev.morling.hardwood.parquet.thrift.ThriftCompactReader;

/**
 * Header for a page in Parquet.
 */
public record PageHeader(PageType type,int uncompressedPageSize,int compressedPageSize,DataPageHeader dataPageHeader,DataPageHeaderV2 dataPageHeaderV2,DictionaryPageHeader dictionaryPageHeader){

public enum PageType {
    DATA_PAGE(0),
    INDEX_PAGE(1),
    DICTIONARY_PAGE(2),
    DATA_PAGE_V2(3);

    private final int thriftValue;

    PageType(int thriftValue) {
        this.thriftValue = thriftValue;
    }

    public static PageType fromThriftValue(int value) {
        for (PageType type : values()) {
            if (type.thriftValue == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown page type: " + value);
    }

    }

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
        PageType type = null;
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
                        type = PageType.fromThriftValue(reader.readI32());
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
                        dataPageHeader = DataPageHeader.read(reader);
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
                        dictionaryPageHeader = DictionaryPageHeader.read(reader);
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 8: // data_page_header_v2
                    if (header.type() == 0x0C) {
                        dataPageHeaderV2 = DataPageHeaderV2.read(reader);
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
