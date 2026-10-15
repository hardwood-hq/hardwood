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

            // 2: unencoded_byte_array_data_bytes (optional list<i64>)
            if (index.unencodedByteArrayDataBytes() != null) {
                writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.LIST);
                writer.writeListBegin(index.unencodedByteArrayDataBytes().length,
                        ThriftCompactConstants.ElementType.I64);
                for (long bytes : index.unencodedByteArrayDataBytes()) {
                    writer.writeI64(bytes);
                }
            }
            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }
}
