/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;

import dev.hardwood.metadata.PageLocation;

/// Reader for PageLocation from Thrift Compact Protocol.
public class PageLocationReader {

    public static PageLocation read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static PageLocation readInternal(ThriftCompactReader reader) throws IOException {
        long offset = 0;
        int compressedPageSize = 0;
        long firstRowIndex = 0;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // offset (i64)
                    if (header.type() == 0x06) {
                        offset = reader.readI64();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // compressed_page_size (i32)
                    if (header.type() == 0x05) {
                        compressedPageSize = reader.readI32();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 3: // first_row_index (i64)
                    if (header.type() == 0x06) {
                        firstRowIndex = reader.readI64();
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

        return new PageLocation(offset, compressedPageSize, firstRowIndex);
    }
}
