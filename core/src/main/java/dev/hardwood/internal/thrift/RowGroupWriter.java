/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;

/// Writer for RowGroup to Thrift Compact Protocol, the inverse of
/// [RowGroupReader].
public class RowGroupWriter {

    public static void write(ThriftCompactWriter writer, RowGroup rowGroup) {
        short saved = writer.pushFieldIdContext();
        try {
            // 1: columns (list<ColumnChunk>)
            writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.LIST);
            writer.writeListBegin(rowGroup.columns().size(), ThriftCompactConstants.ElementType.STRUCT);
            for (ColumnChunk chunk : rowGroup.columns()) {
                ColumnChunkWriter.write(writer, chunk);
            }

            // 2: total_byte_size
            writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.I64);
            writer.writeI64(rowGroup.totalByteSize());

            // 3: num_rows
            writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.I64);
            writer.writeI64(rowGroup.numRows());

            if (rowGroup.fileOffset() != null) {
                writer.writeFieldBegin(5, ThriftCompactConstants.FieldType.I64);
                writer.writeI64(rowGroup.fileOffset());
            }
            if (rowGroup.totalCompressedSize() != null) {
                writer.writeFieldBegin(6, ThriftCompactConstants.FieldType.I64);
                writer.writeI64(rowGroup.totalCompressedSize());
            }
            if (rowGroup.ordinal() != null) {
                writer.writeFieldBegin(7, ThriftCompactConstants.FieldType.I16);
                writer.writeI32(rowGroup.ordinal());
            }

            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }
}
