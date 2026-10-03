/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;

/// Writer for `OffsetIndex` to Thrift Compact Protocol, the inverse of [OffsetIndexReader].
/// `unencoded_byte_array_data_bytes` (field 2) is not written.
public class OffsetIndexWriter {

    public static void write(ThriftCompactWriter writer, OffsetIndex index) {
        short saved = writer.pushFieldIdContext();
        try {
            // 1: page_locations (required list<PageLocation>)
            writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.LIST);
            writer.writeListBegin(index.pageLocations().size(), ThriftCompactConstants.ElementType.STRUCT);
            for (PageLocation location : index.pageLocations()) {
                short element = writer.pushFieldIdContext();
                writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I64);
                writer.writeI64(location.offset());
                writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32);
                writer.writeI32(location.compressedPageSize());
                writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.I64);
                writer.writeI64(location.firstRowIndex());
                writer.writeFieldStop();
                writer.popFieldIdContext(element);
            }
            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }
}
