/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

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

        // 6: created_by (optional)
        if (metaData.createdBy() != null) {
            writer.writeFieldBegin(6, ThriftCompactConstants.FieldType.BINARY);
            writer.writeString(metaData.createdBy());
        }

        writer.writeFieldStop();
    }
}
