/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.Collections;
import java.util.List;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;

/// Reader for OffsetIndex from Thrift Compact Protocol.
///
/// Parquet OffsetIndex struct fields:
///
/// - 1: page_locations (list<PageLocation>)
/// - 2: unencoded_byte_array_data_bytes (list<i64>, optional)
public class OffsetIndexReader {

    public static OffsetIndex read(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.OFFSET_INDEX);
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static OffsetIndex readInternal(ThriftCompactReader reader) {
        List<PageLocation> pageLocations = Collections.emptyList();
        long[] unencodedByteArrayDataBytes = null;
        boolean pageLocationsSeen = false;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            switch (ThriftCompactReader.fieldId(header)) {
                case 1: // page_locations (required list<PageLocation>) — so a wrong wire type fails here
                    reader.requireField(header, Codes.LIST);
                    pageLocations = reader.readStructList(PageLocationReader::read);
                    pageLocationsSeen = true;
                    break;
                case 2: // unencoded_byte_array_data_bytes (list<i64>, optional)
                    if (reader.acceptField(header, Codes.LIST)) {
                        unencodedByteArrayDataBytes = reader.readOptionalI64Array();
                    }
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        if (!pageLocationsSeen) {
            throw ThriftCompactReader.missingFields(ThriftStruct.OFFSET_INDEX, 1);
        }

        return new OffsetIndex(pageLocations, unencodedByteArrayDataBytes);
    }
}
