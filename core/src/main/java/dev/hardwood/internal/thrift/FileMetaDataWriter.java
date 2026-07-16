/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.Map;

import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SchemaElement;

/// Writer for FileMetaData (the file footer struct) to Thrift Compact Protocol,
/// the inverse of [FileMetaDataReader].
public class FileMetaDataWriter {

    public static void write(ThriftCompactWriter writer, FileMetaData metaData) {
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
            RowGroupWriter.write(writer, rowGroup);
        }

        // 5: key_value_metadata (optional list<KeyValue>)
        if (!metaData.keyValueMetadata().isEmpty()) {
            writer.writeFieldBegin(5, ThriftCompactConstants.FieldType.LIST);
            writer.writeListBegin(metaData.keyValueMetadata().size(), ThriftCompactConstants.ElementType.STRUCT);
            for (Map.Entry<String, String> entry : metaData.keyValueMetadata().entrySet()) {
                writeKeyValue(writer, entry.getKey(), entry.getValue());
            }
        }

        // 6: created_by (optional)
        if (metaData.createdBy() != null) {
            writer.writeFieldBegin(6, ThriftCompactConstants.FieldType.BINARY);
            writer.writeString(metaData.createdBy());
        }

        // 7: column_orders (optional list<ColumnOrder>). UNKNOWN cannot be
        // represented faithfully, so omit the complete optional list in that case.
        if (!metaData.columnOrders().isEmpty()
                && !metaData.columnOrders().contains(ColumnOrder.UNKNOWN)) {
            writer.writeFieldBegin(7, ThriftCompactConstants.FieldType.LIST);
            writer.writeListBegin(metaData.columnOrders().size(), ThriftCompactConstants.ElementType.STRUCT);
            for (ColumnOrder order : metaData.columnOrders()) {
                writeColumnOrder(writer, order);
            }
        }

        writer.writeFieldStop();
    }

    private static void writeKeyValue(ThriftCompactWriter writer, String key, String value) {
        short saved = writer.pushFieldIdContext();
        try {
            writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.BINARY);
            writer.writeString(key);
            if (value != null) {
                writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.BINARY);
                writer.writeString(value);
            }
            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }

    private static void writeColumnOrder(ThriftCompactWriter writer, ColumnOrder order) {
        short saved = writer.pushFieldIdContext();
        try {
            int fieldId = order == ColumnOrder.TYPE_DEFINED_ORDER ? 1 : 2;
            writer.writeFieldBegin(fieldId, ThriftCompactConstants.FieldType.STRUCT);
            short marker = writer.pushFieldIdContext();
            writer.writeFieldStop();
            writer.popFieldIdContext(marker);
            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }
}
