/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.PageLocation;

/// Reader for PageLocation from Thrift Compact Protocol.
public class PageLocationReader {

    /// Ids of the fields the format requires of a `PageLocation`: all of them.
    private static final int[] REQUIRED_FIELDS = { 1, 2, 3 };

    public static PageLocation read(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.PAGE_LOCATION);
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static PageLocation readInternal(ThriftCompactReader reader) {
        long offset = 0;
        int compressedPageSize = 0;
        long firstRowIndex = 0;
        long seen = 0;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            int fieldId = ThriftCompactReader.fieldId(header);
            switch (fieldId) {
                // Every field is required, so a wrong wire type fails here rather than being
                // reported as a field that never arrived.
                case 1: // offset (i64)
                    reader.requireField(header, Codes.I64);
                    offset = reader.readNonNegativeI64();
                    seen |= 1L << fieldId;
                    break;
                case 2: // compressed_page_size (i32)
                    reader.requireField(header, Codes.I32);
                    compressedPageSize = reader.readNonNegativeI32();
                    seen |= 1L << fieldId;
                    break;
                case 3: // first_row_index (i64)
                    reader.requireField(header, Codes.I64);
                    firstRowIndex = reader.readNonNegativeI64();
                    seen |= 1L << fieldId;
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        ThriftCompactReader.requireFields(ThriftStruct.PAGE_LOCATION, seen, REQUIRED_FIELDS);

        return new PageLocation(offset, compressedPageSize, firstRowIndex);
    }
}
