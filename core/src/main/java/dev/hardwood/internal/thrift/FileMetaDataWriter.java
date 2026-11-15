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
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SchemaElement;

/// Writer for FileMetaData (the file footer struct) to Thrift Compact Protocol,
/// the inverse of [FileMetaDataReader].
public class FileMetaDataWriter {

    public static void write(ThriftCompactWriter writer, FileMetaData metaData) {
        write(writer, metaData, List.of());
    }

    /// Writes `metaData` with the order every row group's rows are sorted in, which is omitted
    /// where empty.
    public static void write(ThriftCompactWriter writer, FileMetaData metaData,
                             List<SortingColumnIndex> sortingColumns) {
        writer.pushFieldIdContext();

        // 1: version
        writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(metaData.version());

        // 2: schema (list<SchemaElement>)
        writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.LIST);
        writer.writeListBegin(metaData.schema().size(), ThriftCompactConstants.ElementType.STRUCT);
        for (SchemaElement element : metaData.schema()) {
            SchemaElementWriter.write(writer, element);
        }

        // 3: num_rows
        writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.I64);
        writer.writeI64(metaData.numRows());

        // 4: row_groups (list<RowGroup>)
        writer.writeFieldBegin(4, ThriftCompactConstants.FieldType.LIST);
        writer.writeListBegin(metaData.rowGroups().size(), ThriftCompactConstants.ElementType.STRUCT);
        for (RowGroup rowGroup : metaData.rowGroups()) {
            RowGroupWriter.write(writer, rowGroup, sortingColumns);
        }

        // 5: key_value_metadata (optional list<KeyValue>), the application metadata the
        // caller stamped on the file. Omitted entirely when empty, so a file that was given
        // none is byte-identical to one written before the field existed.
        if (!metaData.keyValueMetadata().isEmpty()) {
            writer.writeFieldBegin(5, ThriftCompactConstants.FieldType.LIST);
            KeyValueMetadataWriter.write(writer, metaData.keyValueMetadata());
        }

        // 6: created_by (optional)
        if (metaData.createdBy() != null) {
            writer.writeFieldBegin(6, ThriftCompactConstants.FieldType.BINARY);
            writer.writeString(metaData.createdBy());
        }

        // 7: column_orders (optional list<ColumnOrder>), one entry per leaf column. Required by
        // the format wherever Statistics min_value / max_value are written, which would
        // otherwise have no defined meaning.
        if (!metaData.columnOrders().isEmpty()) {
            writer.writeFieldBegin(7, ThriftCompactConstants.FieldType.LIST);
            writer.writeListBegin(metaData.columnOrders().size(), ThriftCompactConstants.ElementType.STRUCT);
            for (ColumnOrder order : metaData.columnOrders()) {
                ColumnOrderWriter.write(writer, order);
            }
        }

        writer.writeFieldStop();
    }
}
