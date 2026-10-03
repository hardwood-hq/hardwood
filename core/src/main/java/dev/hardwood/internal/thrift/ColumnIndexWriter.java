/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.List;

import dev.hardwood.metadata.ColumnIndex;

/// Writer for `ColumnIndex` to Thrift Compact Protocol, the inverse of [ColumnIndexReader].
/// Writes the four required members, then `null_counts` and `nan_counts` where present; the
/// level histograms (fields 6 and 7) are not written.
public class ColumnIndexWriter {

    public static void write(ThriftCompactWriter writer, ColumnIndex index) {
        short saved = writer.pushFieldIdContext();
        try {
            // 1: null_pages (required list<bool>)
            boolean[] nullPages = index.nullPages();
            writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.LIST);
            writer.writeListBegin(nullPages.length, ThriftCompactConstants.ElementType.BOOL);
            for (boolean nullPage : nullPages) {
                writer.writeByte(nullPage
                        ? ThriftCompactConstants.FieldType.BOOLEAN_TRUE.code()
                        : ThriftCompactConstants.FieldType.BOOLEAN_FALSE.code());
            }

            // 2: min_values, 3: max_values (required list<binary>)
            writeBinaries(writer, 2, index.minValues());
            writeBinaries(writer, 3, index.maxValues());

            // 4: boundary_order (required enum)
            writer.writeFieldBegin(4, ThriftCompactConstants.FieldType.I32);
            writer.writeI32(boundaryOrder(index.boundaryOrder()));

            // 5: null_counts (optional list<i64>)
            if (index.nullCounts() != null) {
                writeLongs(writer, 5, index.nullCounts());
            }

            // 8: nan_counts (optional list<i64>)
            if (index.nanCounts() != null) {
                writeLongs(writer, 8, index.nanCounts());
            }

            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }

    private static void writeBinaries(ThriftCompactWriter writer, int fieldId, List<byte[]> values) {
        writer.writeFieldBegin(fieldId, ThriftCompactConstants.FieldType.LIST);
        writer.writeListBegin(values.size(), ThriftCompactConstants.ElementType.BINARY);
        for (byte[] value : values) {
            writer.writeBinary(value);
        }
    }

    private static void writeLongs(ThriftCompactWriter writer, int fieldId, long[] values) {
        writer.writeFieldBegin(fieldId, ThriftCompactConstants.FieldType.LIST);
        writer.writeListBegin(values.length, ThriftCompactConstants.ElementType.I64);
        for (long value : values) {
            writer.writeI64(value);
        }
    }

    private static int boundaryOrder(ColumnIndex.BoundaryOrder order) {
        return switch (order) {
            case UNORDERED -> 0;
            case ASCENDING -> 1;
            case DESCENDING -> 2;
        };
    }
}
