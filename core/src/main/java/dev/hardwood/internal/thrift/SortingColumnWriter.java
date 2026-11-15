/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.List;

import dev.hardwood.internal.metadata.SortingColumnIndex;

/// Writer for a row group's `sorting_columns` (`list<SortingColumn>`) to Thrift Compact Protocol.
final class SortingColumnWriter {

    private SortingColumnWriter() {
    }

    /// Writes the list body, the field header having been written by the caller.
    static void writeList(ThriftCompactWriter writer, List<SortingColumnIndex> sortingColumns) {
        writer.writeListBegin(sortingColumns.size(), ThriftCompactConstants.ElementType.STRUCT);
        for (SortingColumnIndex column : sortingColumns) {
            short saved = writer.pushFieldIdContext();
            try {
                // 1: column_idx (required i32)
                writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
                writer.writeI32(column.columnIndex());
                // 2: descending (required bool)
                writer.writeBool(2, column.descending());
                // 3: nulls_first (required bool)
                writer.writeBool(3, column.nullsFirst());
                writer.writeFieldStop();
            }
            finally {
                writer.popFieldIdContext(saved);
            }
        }
    }
}
