/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.metadata.ColumnIndex;

/// Reader for ColumnIndex from Thrift Compact Protocol.
///
/// Parquet ColumnIndex struct fields:
///
/// - 1: null_pages (list<bool>)
/// - 2: min_values (list<binary>)
/// - 3: max_values (list<binary>)
/// - 4: boundary_order (enum BoundaryOrder)
/// - 5: null_counts (list<i64>, optional)
public class ColumnIndexReader {

    public static ColumnIndex read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static ColumnIndex readInternal(ThriftCompactReader reader) throws IOException {
        List<Boolean> nullPages = new ArrayList<>();
        List<byte[]> minValues = new ArrayList<>();
        List<byte[]> maxValues = new ArrayList<>();
        ColumnIndex.BoundaryOrder boundaryOrder = ColumnIndex.BoundaryOrder.UNORDERED;
        List<Long> nullCounts = null;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // null_pages (list<bool>)
                    if (header.type() == 0x09) { // LIST
                        ThriftCompactReader.CollectionHeader listHeader = reader.readListHeader();
                        for (int i = 0; i < listHeader.size(); i++) {
                            nullPages.add(reader.readBoolean());
                        }
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // min_values (list<binary>)
                    if (header.type() == 0x09) { // LIST
                        ThriftCompactReader.CollectionHeader listHeader = reader.readListHeader();
                        for (int i = 0; i < listHeader.size(); i++) {
                            minValues.add(reader.readBinary());
                        }
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 3: // max_values (list<binary>)
                    if (header.type() == 0x09) { // LIST
                        ThriftCompactReader.CollectionHeader listHeader = reader.readListHeader();
                        for (int i = 0; i < listHeader.size(); i++) {
                            maxValues.add(reader.readBinary());
                        }
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 4: // boundary_order (enum)
                    if (header.type() == 0x05) { // I32
                        int val = reader.readI32();
                        boundaryOrder = switch (val) {
                            case 1 -> ColumnIndex.BoundaryOrder.ASCENDING;
                            case 2 -> ColumnIndex.BoundaryOrder.DESCENDING;
                            default -> ColumnIndex.BoundaryOrder.UNORDERED;
                        };
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 5: // null_counts (list<i64>, optional)
                    if (header.type() == 0x09) { // LIST
                        ThriftCompactReader.CollectionHeader listHeader = reader.readListHeader();
                        nullCounts = new ArrayList<>(listHeader.size());
                        for (int i = 0; i < listHeader.size(); i++) {
                            nullCounts.add(reader.readI64());
                        }
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

        return new ColumnIndex(nullPages, minValues, maxValues, boundaryOrder, nullCounts);
    }
}
