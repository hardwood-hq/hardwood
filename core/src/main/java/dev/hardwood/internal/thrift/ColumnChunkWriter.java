/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.metadata.ColumnChunk;

/// Writer for ColumnChunk to Thrift Compact Protocol, the inverse of
/// [ColumnChunkReader].
public class ColumnChunkWriter {

    public static void write(ThriftCompactWriter writer, ColumnChunk chunk) {
        short saved = writer.pushFieldIdContext();
        try {
            // 2: file_offset (required). For a single-page chunk with no dictionary
            // this is the offset of the data page, matching data_page_offset.
            writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.I64);
            writer.writeI64(chunk.metaData().dataPageOffset());

            // 3: meta_data
            writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.STRUCT);
            ColumnMetaDataWriter.write(writer, chunk.metaData());

            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }
}
